from google.cloud import bigquery
from google.cloud.exceptions import NotFound

PROJECT_ID = "taxi-trip-analytics-478020"

client = bigquery.Client(project=PROJECT_ID,location="US" )

datasets_name = ['raw_yellowtrips', 'transformed_data', 'views_fordashboard']



def create_bigquery_dataset(dataset_name):
    """Create bigquery dataset. Check first if the dataset exists
        Args:
            dataset_name: String
    """

    dataset_id = "{}.{}".format(client.project, dataset_name)
    try:
        client.get_dataset(dataset_id)
        print("Dataset {} already exists".format(dataset_id))
    except NotFound:
        dataset = bigquery.Dataset(dataset_id)
        #dataset.location = location
        # Make an API request.
        dataset = client.create_dataset(dataset, timeout=30)
        print("Created dataset {}.{}".format(
            client.project, dataset.dataset_id))


for name in datasets_name:
    create_bigquery_dataset(name)