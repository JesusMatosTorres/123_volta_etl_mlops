# Databricks notebook source
task_name = dbutils.widgets.get("task_name")

print(f"Ejecutando Task: {task_name}")

dbutils.notebook.run(task_name, timeout_seconds=3600)