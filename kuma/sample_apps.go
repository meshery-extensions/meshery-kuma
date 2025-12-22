package kuma

import (
	"context"
	"sync"

	"github.com/layer5io/meshery-adapter-library/adapter"
	"github.com/layer5io/meshery-adapter-library/status"
	mesherykube "github.com/layer5io/meshkit/utils/kubernetes"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (kuma *Kuma) installSampleApp(del bool, namespace string, templates []adapter.Template, kubeconfigs []string) (string, error) {
	st := status.Installing

	if del {
		st = status.Removing
	}

	for _, template := range templates {
		err := kuma.applyManifest(del, namespace, []byte(template.String()), kubeconfigs)
		if err != nil {
			return st, ErrSampleApp(err, st)
		}
	}

	return status.Installed, nil
}

func (kuma *Kuma) sidecarInjection(namespace string, del bool, kubeconfigs []string) error {
	var wg sync.WaitGroup
	var errs []error
	var errMx sync.Mutex
	for _, kubeconfig := range kubeconfigs {
		wg.Add(1)
		go func(kubeconfig string) {
			defer wg.Done()
			kClient, err := mesherykube.New([]byte(kubeconfig))
			if err != nil {
				errMx.Lock()
				errs = append(errs, err)
				errMx.Unlock()
				return
			}
			// updating the label on the namespace
			ns, err := kClient.KubeClient.CoreV1().Namespaces().Get(context.TODO(), namespace, metav1.GetOptions{})
			if err != nil {
				errMx.Lock()
				errs = append(errs, err)
				errMx.Unlock()
				return
			}

			// updating the annotations on the namespace
			if ns.Annotations == nil {
				ns.Annotations = map[string]string{}
			}
			ns.Annotations["kuma.io/sidecar-injection"] = "enabled"

			if del {
				delete(ns.Annotations, "kuma.io/sidecar-injection")
			}

			_, err = kClient.KubeClient.CoreV1().Namespaces().Update(context.TODO(), ns, metav1.UpdateOptions{})
			if err != nil {
				errMx.Lock()
				errs = append(errs, err)
				errMx.Unlock()
				return
			}
		}(kubeconfig)
	}
	wg.Wait()
	if len(errs) != 0 {
		return ErrLoadNamespace(mergeErrors(errs), namespace)
	}
	return nil
}
