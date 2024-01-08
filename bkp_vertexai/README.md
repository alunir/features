# vertexai
Custom Containers for Vertex AI

## References
 - [カスタム トレーニング用の事前にビルドされたコンテナ](https://cloud.google.com/vertex-ai/docs/training/pre-built-containers)
 - [Vertex AI で Deep Learning VM Image と Deep Learning Containers を使用する](https://cloud.google.com/vertex-ai/docs/general/deep-learning)
 - [トレーニング用のカスタム コンテナ イメージを作成する](https://cloud.google.com/vertex-ai/docs/training/create-custom-container)
 - [トレーニング アプリケーションからデータセットにアクセスする](https://cloud.google.com/vertex-ai/docs/training/using-managed-datasets#access_a_dataset_from_your_training_application)
> AIP_DATA_FORMAT: データセットのエクスポート形式。有効な値は jsonl、csv、bigquery です。
AIP_TRAINING_DATA_URI: トレーニング データの保存場所。
AIP_VALIDATION_DATA_URI: 検証データの保存場所。
AIP_TEST_DATA_URI: テストデータの保存場所。
 - [Tabular data](https://cloud.google.com/vertex-ai/docs/training-overview#tabular_data)
> Vertex AIでは、シンプルなプロセスとインターフェースを用いて、表形式データで機械学習を行うことができます。表形式データの問題に対して、以下のモデルタイプを作成することができます。
二値分類モデルは、二値結果（2つのクラスのうちの1つ）を予測します。このモデル・タイプは、「はい」か「いいえ」の質問に使用します。例えば、顧客がサブスクリプションを購入するかどうかを予測するために、バイナリ分類モデルを構築したい場合があります。一般に、バイナリ分類の問題は、他のモデル・タイプよりも少ないデータしか必要としません。
多クラス分類モデルは、3つ以上の離散クラスから1つのクラスを予測します。このモデル・タイプは、分類に使用します。例えば、小売業では、顧客を異なるペルソナにセグメンテーションするために、マルチクラス分類モデルを構築することが望まれます。
回帰モデルは、連続値を予測します。例えば、小売業では、顧客が来月にいくら使うかを予測する回帰モデルを構築することができます。
予測モデルは、一連の値を予測します。例えば、小売業者であれば、今後3ヶ月間の商品の需要を予測し、事前に商品の在庫を適切に確保することができます。
 - [Vertex AI にモデルをインポートする](https://cloud.google.com/vertex-ai/docs/model-registry/import-model)

