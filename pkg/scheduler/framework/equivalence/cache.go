package equivalence

import (
	"context"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/cache"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
)

const cacheTTL = time.Minute
const cacheSize = 100 * 1000

func NewCachableFilterPlugin(plugin framework.FilterPlugin) framework.FilterPlugin {
	if c, ok := plugin.(framework.CachablePlugin); ok {
		return &filterCacheImpl{
			FilterPlugin: plugin,
			history:      cache.NewLRUExpireCache(cacheSize),
			cachable:     c,
		}
	}
	return plugin
}

func NewCachableScorePlugin(plugin framework.ScorePlugin, snapshotLister framework.SharedLister) framework.ScorePlugin {
	if c, ok := plugin.(framework.CachablePlugin); ok {
		return &scoreCacheImpl{
			ScorePlugin:    plugin,
			history:        cache.NewLRUExpireCache(cacheSize),
			cachable:       c,
			snapshotLister: snapshotLister,
		}
	}
	return plugin
}

type filterCacheImpl struct {
	framework.FilterPlugin
	history  *cache.LRUExpireCache
	cachable framework.CachablePlugin
}

type cacheKey struct {
	nodeHash string
	podHash  string
}

func (i *filterCacheImpl) Filter(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
	nodeHash := i.cachable.NodeEquivalenceHashFunc()(nodeInfo)
	podHash := i.cachable.PodEquivalenceHashFunc()(pod)
	k := cacheKey{
		nodeHash: string(nodeHash),
		podHash:  string(podHash),
	}
	v, ok := i.history.Get(k)
	if !ok {
		metrics.EquivalenceCacheHits.With(map[string]string{
			"plugin": i.FilterPlugin.Name(),
			"result": "miss",
		}).Inc()
		status := i.FilterPlugin.Filter(ctx, state, pod, nodeInfo)
		defer i.history.Add(k, status, cacheTTL)
		return status
	}
	metrics.EquivalenceCacheHits.With(map[string]string{
		"plugin": i.FilterPlugin.Name(),
		"result": "hit",
	}).Inc()
	return v.(*framework.Status)
}

type scoreCacheImpl struct {
	framework.ScorePlugin
	history        *cache.LRUExpireCache
	cachable       framework.CachablePlugin
	snapshotLister framework.SharedLister
}

type scoreCacheValue struct {
	score  int64
	status *framework.Status
}

func (i *scoreCacheImpl) Score(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) (int64, *framework.Status) {
	nodeInfo, err := i.snapshotLister.NodeInfos().Get(nodeName)
	if err != nil {
		metrics.EquivalenceCacheHits.With(map[string]string{
			"plugin": i.ScorePlugin.Name(),
			"result": "error",
		}).Inc()
		return i.Score(ctx, state, pod, nodeName)
	}
	nodeHash := i.cachable.NodeEquivalenceHashFunc()(nodeInfo)
	podHash := i.cachable.PodEquivalenceHashFunc()(pod)
	k := cacheKey{
		nodeHash: string(nodeHash),
		podHash:  string(podHash),
	}
	v, ok := i.history.Get(k)
	if !ok {
		metrics.EquivalenceCacheHits.With(map[string]string{
			"plugin": i.ScorePlugin.Name(),
			"result": "miss",
		}).Inc()
		score, status := i.ScorePlugin.Score(ctx, state, pod, nodeName)
		defer i.history.Add(k, &scoreCacheValue{
			score:  score,
			status: status,
		}, cacheTTL)
		return score, status
	}
	metrics.EquivalenceCacheHits.With(map[string]string{
		"plugin": i.ScorePlugin.Name(),
		"result": "hit",
	}).Inc()
	return v.(*scoreCacheValue).score, v.(*scoreCacheValue).status
}
