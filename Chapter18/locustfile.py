from locust import HttpLocust, TaskSet, task


class WebsiteTasks(TaskSet):
    @task
    def preduct(self):
        self.client.get(
            "/predict/residential?latitude=40.675719430504&longitude=-73.860535138411&created_date=2019-06-14T00%3A02%3A11.000"
        )

    @task
    def preduct_async(self):
        self.client.get(
            "/predict_async/residential?latitude=40.675719430504&longitude=-73.860535138411&created_date=2019-06-14T00%3A02%3A11.000"
        )

    @task
    def dashboard(self):
        self.client.get("/dashboard/dashboard")


class WebsiteUser(HttpLocust):

    task_set = WebsiteTasks
    min_wait = 5000
    max_wait = 15000
