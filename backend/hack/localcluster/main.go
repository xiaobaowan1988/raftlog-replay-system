// Command localcluster starts a real kube-apiserver + etcd via envtest, installs
// the CRDs in the given directory, writes a kubeconfig, and blocks so another
// process (here, the backend) can apply against it. It's a test control plane —
// no nodes/kubelet — driven by hack/local-apiserver-test.sh.
//
// Its own module (heavy controller-runtime test deps) so it never touches the
// backend's dependency graph.
//
// Usage: KUBEBUILDER_ASSETS=<dir> go run . <crd-dir> <kubeconfig-out>
package main

import (
	"fmt"
	"os"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

func main() {
	crdDir := os.Args[1]
	kubeconfigOut := os.Args[2]

	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{crdDir},
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := env.Start()
	if err != nil {
		fmt.Fprintln(os.Stderr, "envtest start:", err)
		os.Exit(1)
	}
	if err := writeKubeconfig(cfg, kubeconfigOut); err != nil {
		fmt.Fprintln(os.Stderr, "write kubeconfig:", err)
		os.Exit(1)
	}
	fmt.Println("APISERVER", cfg.Host)
	fmt.Println("KUBECONFIG", kubeconfigOut)
	fmt.Println("READY")
	select {} // keep the control plane alive
}

func writeKubeconfig(cfg *rest.Config, path string) error {
	c := clientcmdapi.NewConfig()
	c.Clusters["envtest"] = &clientcmdapi.Cluster{
		Server:                   cfg.Host,
		CertificateAuthorityData: cfg.CAData,
	}
	c.AuthInfos["envtest"] = &clientcmdapi.AuthInfo{
		ClientCertificateData: cfg.CertData,
		ClientKeyData:         cfg.KeyData,
	}
	c.Contexts["envtest"] = &clientcmdapi.Context{Cluster: "envtest", AuthInfo: "envtest"}
	c.CurrentContext = "envtest"
	return clientcmd.WriteToFile(*c, path)
}
