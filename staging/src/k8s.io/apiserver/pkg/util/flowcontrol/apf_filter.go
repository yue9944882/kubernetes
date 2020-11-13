/*
Copyright 2019 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package flowcontrol

import (
	"context"
	"strconv"
	"time"

	"k8s.io/apimachinery/pkg/util/clock"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/server/mux"
	"k8s.io/apiserver/pkg/util/flowcontrol/counter"
	fq "k8s.io/apiserver/pkg/util/flowcontrol/fairqueuing"
	fqs "k8s.io/apiserver/pkg/util/flowcontrol/fairqueuing/queueset"
	"k8s.io/apiserver/pkg/util/flowcontrol/metrics"
	kubeinformers "k8s.io/client-go/informers"
	"k8s.io/klog/v2"

	flowcontrol "k8s.io/api/flowcontrol/v1beta1"
	flowcontrolclient "k8s.io/client-go/kubernetes/typed/flowcontrol/v1beta1"
)

// Interface defines how the API Priority and Fairness filter interacts with the underlying system.
type Interface interface {
	// Handle takes care of queuing and dispatching a request
	// characterized by the given digest.  The given `noteFn` will be
	// invoked with the results of request classification.  If the
	// request is queued then `queueNoteFn` will be called twice,
	// first with `true` and then with `false`; otherwise
	// `queueNoteFn` will not be called at all.  If Handle decides
	// that the request should be executed then `execute()` will be
	// invoked once to execute the request; otherwise `execute()` will
	// not be invoked.
	Handle(ctx context.Context,
		requestDigest RequestDigest,
		noteFn func(fs *flowcontrol.FlowSchema, pl *flowcontrol.PriorityLevelConfiguration),
		queueNoteFn fq.QueueNoteFn,
		execFn func(),
	)

	// MaintainObservations is a helper for maintaining statistics.
	MaintainObservations(stopCh <-chan struct{})

	// Run monitors config objects from the main apiservers and causes
	// any needed changes to local behavior.  This method ceases
	// activity and returns after the given channel is closed.
	Run(stopCh <-chan struct{}) error

	// Install installs debugging endpoints to the web-server.
	Install(c *mux.PathRecorderMux)
}

// This request filter implements https://github.com/kubernetes/enhancements/blob/master/keps/sig-api-machinery/20190228-priority-and-fairness.md

// New creates a new instance to implement API priority and fairness
func New(
	informerFactory kubeinformers.SharedInformerFactory,
	flowcontrolClient flowcontrolclient.FlowcontrolV1beta1Interface,
	serverConcurrencyLimit int,
	requestWaitLimit time.Duration,
) Interface {
	grc := counter.NoOp{}
	return NewTestable(
		informerFactory,
		flowcontrolClient,
		serverConcurrencyLimit,
		requestWaitLimit,
		metrics.PriorityLevelConcurrencyObserverPairGenerator,
		fqs.NewQueueSetFactory(&clock.RealClock{}, grc),
	)
}

// NewTestable is extra flexible to facilitate testing
func NewTestable(
	informerFactory kubeinformers.SharedInformerFactory,
	flowcontrolClient flowcontrolclient.FlowcontrolV1beta1Interface,
	serverConcurrencyLimit int,
	requestWaitLimit time.Duration,
	obsPairGenerator metrics.TimedObserverPairGenerator,
	queueSetFactory fq.QueueSetFactory,
) Interface {
	return newTestableController(informerFactory, flowcontrolClient, serverConcurrencyLimit, requestWaitLimit, obsPairGenerator, queueSetFactory)
}

// appendUnauthenticatedGroupIfNotInKnownGroups modifies the passed request
// digest to append system:unauthenticated if the none of the user's groups
// matches the list of known groups.
func appendUnauthenticatedGroupIfNotInKnownGroups(requestDigest *RequestDigest) {
	wantGroups := []string{user.AllAuthenticated, user.AllUnauthenticated}
	for _, wantGroup := range wantGroups {
		for _, haveGroup := range requestDigest.User.GetGroups() {
			if haveGroup == wantGroup {
				return
			}
		}
	}
	klog.Errorf("request digest with request info %#+v and user %#+v not a part %v, appending %q group", requestDigest.RequestInfo, requestDigest.User, wantGroups, user.AllUnauthenticated)
	requestDigest.User = &user.DefaultInfo{
		Name:   requestDigest.User.GetName(),
		UID:    requestDigest.User.GetUID(),
		Groups: append(requestDigest.User.GetGroups(), user.AllUnauthenticated),
		Extra:  requestDigest.User.GetExtra(),
	}
}

// assumeAnonymousUserIfNotPresent modifies the passed request digest to set
// the user's name if unset.
func assumeAnonymousUserIfUnset(requestDigest *RequestDigest) {
	if requestDigest.User.GetName() == "" {
		klog.Errorf("request digest with request info %#+v and user %#+v has no name, assuming %q", requestDigest.RequestInfo, requestDigest.User, user.Anonymous)
		requestDigest.User = &user.DefaultInfo{
			Name:   user.Anonymous,
			UID:    requestDigest.User.GetUID(),
			Groups: requestDigest.User.GetGroups(),
			Extra:  requestDigest.User.GetExtra(),
		}
	}
}

func (cfgCtlr *configController) Handle(ctx context.Context, requestDigest RequestDigest,
	noteFn func(fs *flowcontrol.FlowSchema, pl *flowcontrol.PriorityLevelConfiguration),
	queueNoteFn fq.QueueNoteFn,
	execFn func()) {
	assumeAnonymousUserIfUnset(&requestDigest)
	appendUnauthenticatedGroupIfNotInKnownGroups(&requestDigest)
	fs, pl, isExempt, req, startWaitingTime := cfgCtlr.startRequest(ctx, requestDigest, queueNoteFn)
	queued := startWaitingTime != time.Time{}
	noteFn(fs, pl)
	if req == nil {
		if queued {
			metrics.ObserveWaitingDuration(pl.Name, fs.Name, strconv.FormatBool(req != nil), time.Since(startWaitingTime))
		}
		klog.V(7).Infof("Handle(%#+v) => fsName=%q, distMethod=%#+v, plName=%q, isExempt=%v, reject", requestDigest, fs.Name, fs.Spec.DistinguisherMethod, pl.Name, isExempt)
		return
	}
	klog.V(7).Infof("Handle(%#+v) => fsName=%q, distMethod=%#+v, plName=%q, isExempt=%v, queued=%v", requestDigest, fs.Name, fs.Spec.DistinguisherMethod, pl.Name, isExempt, queued)
	var executed bool
	idle := req.Finish(func() {
		if queued {
			metrics.ObserveWaitingDuration(pl.Name, fs.Name, strconv.FormatBool(req != nil), time.Since(startWaitingTime))
		}
		metrics.AddDispatch(pl.Name, fs.Name)
		executed = true
		startExecutionTime := time.Now()
		execFn()
		metrics.ObserveExecutionDuration(pl.Name, fs.Name, time.Since(startExecutionTime))
	})
	if queued && !executed {
		metrics.ObserveWaitingDuration(pl.Name, fs.Name, strconv.FormatBool(req != nil), time.Since(startWaitingTime))
	}
	klog.V(7).Infof("Handle(%#+v) => fsName=%q, distMethod=%#+v, plName=%q, isExempt=%v, queued=%v, Finish() => idle=%v", requestDigest, fs.Name, fs.Spec.DistinguisherMethod, pl.Name, isExempt, queued, idle)
	if idle {
		cfgCtlr.maybeReap(pl.Name)
	}
}
