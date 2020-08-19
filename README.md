# Clustering Ethereum Addresses based on Patterns in the public Transaction-Data 

This project was implemented in the course of my master thesis at the Institute of Applied Informatics and Formal Description Methods (AIFB) at the KIT Department of Economics and Management. 

# Setup
```bash
 	pip install -r requirements.txt
 	mkdir build
 	mkdir secrets 
 	cat > secrets/secrets.json
 	"WEB3_INFURA_PROJECT_ID": "<<Your infura-id>>" 
 	cat > secrets/bigquery-service-account.json
 	{
  		"type": "service_account",
  		"project_id": "project-id",
  		"private_key_id": "key-id",
  		"private_key": "-----BEGIN PRIVATE KEY-----\nprivate-key\n-----END PRIVATE KEY-----\n",
  		"client_email": "service-account-email",
  		"client_id": "client-id",
  		"auth_uri": "https://accounts.google.com/o/oauth2/auth",
  		"token_uri": "https://accounts.google.com/o/oauth2/token",
  		"auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  		"client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/service-account-email"
	}
```

* For the infura-id check out [infura.io](https://infura.io/). 

* For the bigquery-service-account.json check out [cloud.google.com](https://cloud.google.com/iam/docs/creating-managing-service-account-keys). 

---
**Warning**

Once connected to your gcloud service account the execution of the clusteranalysis is going to cost money. You can control the maximum costs with the variable 'max\_bigquery\_costs\_usd' in './clusteranalysis.ipynb'. By default, the maximum costs are restricted to 2 USD.  

---

# Run Experiments

```bash
	python3 main.py
```

Executes 18 different experiments. The experiments are saved in './build'.

```bash
	jupyter notebook
```

Analyse the different experiments done via main.py in './build' or in './expriments'.
