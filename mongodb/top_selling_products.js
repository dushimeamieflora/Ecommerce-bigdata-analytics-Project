[
  {
    '$unwind': '$items'
  }, {
    '$group': {
      '_id': '$items.product_id', 
      'total_quantity_sold': {
        '$sum': '$items.quantity'
      }, 
      'total_revenue': {
        '$sum': '$items.subtotal'
      }
    }
  }, {
    '$lookup': {
      'from': 'products', 
      'localField': '_id', 
      'foreignField': 'product_id', 
      'as': 'product_details'
    }
  }, {
    '$unwind': '$product_details'
  }, {
    '$project': {
      '_id': 0, 
      'product_id': '$_id', 
      'product_name': '$product_details.name', 
      'category_id': '$product_details.category_id', 
      'total_quantity_sold': 1, 
      'total_revenue': {
        '$round': [
          '$total_revenue', 2
        ]
      }, 
      'current_stock': '$product_details.current_stock'
    }
  }, {
    '$sort': {
      'total_quantity_sold': -1
    }
  }, {
    '$limit': 10
  }
]