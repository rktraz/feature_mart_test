# <span style="color:#ff5f27">🌦️ Public Weather Feature Group</span>

Historical weather data from **2000-01-01 00:00:00** for the next cities:
- London
- Paris
- Stockholm
- New York
- Los Angeles
- Singapore
- Sydney
- Hong Kong
- Rome
- Kyiv


# 🧙🏼‍♂️ Method of obtaining data
Requests using API from [open-meteo](https://open-meteo.com).


# 👨🏻‍🏫 Data Sample
![1.png](images/data_preview.png)


# 🗓️ Scheduling
To schedule a Feature Pipeline we are using [GitHub Actions](https://github.com/features/actions).

## ⚙️ GitHub Actions Set Up
1. `feature_pipeline_weather.yml` will set up a workflow for you. It will run every day at 00:00.

2. Get your Hopsworks API Key.

`HOPSWORKS_API_KEY`

![2.png](images/api_key.png)

---
3. Add your `HOPSWORKS_API_KEY` to **Actions secrets and variables**.

![3.png](images/set_up_api.png)
![4.png](images/create_api.png)

---
4. Besides scheduling, you can run your workflow manually.
![5.png](images/github_actions.png)
![6.png](images/feature_pipe_run.png)
