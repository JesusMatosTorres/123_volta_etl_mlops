# Databricks notebook source
import json

task_list = ["./AutoscoutTask", "./CarMarketTask", "./CarplusTask", "./Km0Task", 
             "./MLeonTask", "./MotorEsTask", "./NewVehiclePriceTask"]

dbutils.jobs.taskValues.set(key="task_list", value=task_list)
