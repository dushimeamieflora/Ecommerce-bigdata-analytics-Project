
[
  {
    '$unwind': '$items'
  }, {
    '$group': {
      '_id': '$items.product_id', 
      'total_revenue': {
        '$sum': '$items.subtotal'
      }, 
      'total_quantity_sold': {
        '$sum': '$items.quantity'
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
    '$group': {
      '_id': '$product_details.category_id', 
      'total_revenue': {
        '$sum': '$total_revenue'
      }, 
      'total_quantity_sold': {
        '$sum': '$total_quantity_sold'
      }
    }
  }, {
    '$lookup': {
      'from': 'categories', 
      'localField': '_id', 
      'foreignField': 'category_id', 
      'as': 'category_details'
    }
  }, {
    '$unwind': '$category_details'
  }, {
    '$project': {
      '_id': 0, 
      'category_id': '$_id', 
      'category_name': '$category_details.name', 
      'total_revenue': {
        '$round': [
          '$total_revenue', 2
        ]
      }, 
      'total_quantity_sold': 1
    }
  }, {
    '$sort': {
      'total_revenue': -1
    }
  }
]