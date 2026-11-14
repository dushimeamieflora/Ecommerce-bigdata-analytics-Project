
[
    {
        "$unwind": "$items"
    },
    {
        "$addFields": {
            "transaction_month": {
                "$dateTrunc": {
                    "date": {
                        "$dateFromString": {
                            "dateString": "$timestamp"
                        }
                    },
                    "unit": "month"
                }
            }
        }
    },
    {
        "$lookup": {
            "from": "products",
            "localField": "items.product_id",
            "foreignField": "product_id",
            "as": "product_details"
        }
    },
    {
        "$unwind": "$product_details"
    },
    {
        "$group": {
            "_id": {
                "category_id": "$product_details.category_id",
                "month": "$transaction_month",
                "transaction_id": "$transaction_id"
            },
            "transaction_revenue": {
                "$sum": "$items.subtotal"
            },
            "transaction_quantity": {
                "$sum": "$items.quantity"
            }
        }
    },
    {
        "$group": {
            "_id": {
                "category_id": "$_id.category_id",
                "month": "$_id.month"
            },
            "total_revenue": {
                "$sum": "$transaction_revenue"
            },
            "total_quantity_sold": {
                "$sum": "$transaction_quantity"
            },
            "total_transactions": {
                "$sum": 1
            }
        }
    },
    {
        "$lookup": {
            "from": "categories",
            "localField": "_id.category_id",
            "foreignField": "category_id",
            "as": "category_details"
        }
    },
    {
        "$unwind": "$category_details"
    },
    {
        "$project": {
            "_id": 0,
            "category_id": "$_id.category_id",
            "category_name": "$category_details.name",
            "month": {
                "$dateToString": {
                    "format": "%Y-%m",
                    "date": "$_id.month"
                }
            },
            "total_revenue": {
                "$round": ["$total_revenue", 2]
            },
            "total_quantity_sold": 1,
            "total_transactions": 1
        }
    },
    {
        "$sort": {
            "month": 1,
            "total_revenue": -1
        }
    }
]
