package kubernetes

import (
	"context"
	"fmt"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"
)

func TestIntensiveInformerOps(t *testing.T) {
	cs := fake.NewClientset()
	factory := informers.NewSharedInformerFactory(cs, time.Second*60)
	podInformer := factory.Core().V1().Pods().Informer()
	read := &atomic.Int32{}
	defer func() {
		fmt.Printf("WRITE: %v\n", cache.Write.Load())
		fmt.Printf("READ: %v\n", read.Load())
	}()
	q := workqueue.NewTypedRateLimitingQueue[*corev1.Pod](workqueue.DefaultTypedControllerRateLimiter[*corev1.Pod]())
	podInformer.AddEventHandler(cache.ResourceEventHandlerDetailedFuncs{
		AddFunc: func(obj interface{}, isInInitialList bool) {
			pod := obj.(*corev1.Pod)
			q.Add(pod)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			pod := newObj.(*corev1.Pod)
			q.Add(pod)
		},
		DeleteFunc: func(obj interface{}) {
			pod := obj.(*corev1.Pod)
			q.Add(pod)
		},
	})
	for i := 0; i < 16; i++ {
		go func() {
			for {
				_, _ = q.Get()
				read.Add(1)
				podInformer.GetStore().List()
			}
		}()
	}
	factory.Start(make(chan struct{}))

	time.Sleep(time.Second)
	for i := 0; i < 10; i++ {
		for {
			cs.CoreV1().Pods(RandStringRunes(10)).Create(context.TODO(), &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: RandStringRunes(20),
				},
			}, metav1.CreateOptions{})
		}
	}
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}
