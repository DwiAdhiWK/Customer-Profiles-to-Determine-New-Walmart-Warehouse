# Judul Project
Proyek ini menerapkan proses ETL untuk mengolah data pelanggan Walmart guna mendukung analisis lokasi optimal dalam pembangunan gudang distribusi baru.

## Repository Outline
- P2M3_dwi_adhi_DAG.py - file python yang berupa kodingan dags untuk airflow
- P2M3_dwi_adhi_data_clean.csv - csv data setelah preprocessing
- P2M3_dwi_adhi_data_raw.csv - csv data mentah
- images - folder berupa hasil graph beserta insight nya.
- airflow_ES.yaml - yml settingan airflow, postgres, dan elasticsearch
- env - setting environment airflow
- P2M3_dwi_adhi_conceptual.txt - 5 soal yang dikerjakan
- P2M3_dwi_adhi_DAG_graph.jpg - gambar flow dags
- P2M3_dwi_adhi_ddl - data definition language projek
- P2M3_dwi_adhi_GX.ipynb - notebook great expectation

## Problem Background
Pada kuartal fiskal pertama tahun 2025, Walmart mencatatkan pendapatan sebesar $161,5 miliar, mengalami kenaikan sebesar 6% dibandingkan tahun sebelumnya.

Selain itu, penjualan e-commerce meningkat sebesar 22%, didorong oleh semakin banyaknya pelanggan yang menggunakan layanan pengiriman serta berbelanja dari penjual pihak ketiga (third-party) di platform Walmart.

Melihat potensi ekspansi e-commerce yang terus meningkat, Walmart berencana membangun gudang distribusi baru untuk meningkatkan efisiensi logistik dari sisi Walmart ke penjual pihak ketiga.

Untuk mendukung inisiatif ini, dilakukan analisis profil pelanggan (customer profile analysis) guna menentukan lokasi gudang yang ideal serta memahami perilaku pembelian pelanggan secara lebih mendalam.

## Project Output
Output dari projek ini adalah dashboard yang dibuat di Kibana dan pipeline airflow untuk preprocess data.

## Data
Data ini diambil dari data kaggle yang berjudul Walmart Customer Purchase Behavior Dataset Data ini memiliki 50000 baris dan 12 kolom


## Method
Proyek ini menerapkan proses ETL (Extract, Transform, Load) menggunakan Docker Desktop, PostgreSQL, dan Apache Airflow untuk membangun data pipeline yang terotomatisasi.

Setelah data diproses, hasilnya divisualisasikan menggunakan Elasticsearch dan Kibana untuk memudahkan analisis data secara real-time dan interaktif.

## Stacks
- Python
- pandas, datetime, sqlalchemy, airflow, elasticsearch
- Docker desktop
- PostgreSQL

## Reference
https://www.kaggle.com/datasets/logiccraftbyhimanshi/walmart-customer-purchase-behavior-dataset/data

---