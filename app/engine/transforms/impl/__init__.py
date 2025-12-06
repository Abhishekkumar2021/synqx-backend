from app.engine.transforms.factory import TransformFactory
from app.engine.transforms.impl.pandas_transform import PandasTransform

TransformFactory.register_transform("pandas_transform", PandasTransform)
