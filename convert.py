#!/usr/bin/env python3
"""
Convert the new comprehensive store JSON to the format expected by existing code
Creates both branch_urls.json and store_names.json
"""
import json

def convert_store_data():
    """Convert your comprehensive store data to the expected formats"""
    
    # Read your comprehensive store data
    with open('cocopan_stores.json', 'r') as f:
        store_data = json.load(f)
    
    # Extract URLs for branch_urls.json (existing code expects this format)
    urls = []
    store_names_map = {}  # URL -> Store Info mapping
    
    # Process GrabFood stores
    grabfood_data = store_data['cocopan_stores']['grab_food']
    
    # Main GrabFood stores
    for store in grabfood_data['main_list']:
        url = store['url'].rstrip('?')  # Clean trailing ?
        urls.append(url)
        store_names_map[url] = {
            'id': store['id'],
            'store_name': store['store_name'],
            'name_on_platform': store['name_on_platform'],
            'location': store['location'],
            'platform': 'grabfood'
        }
    
    # Additional GrabFood stores
    for store in grabfood_data['additional_stores']:
        url = store['url'].rstrip('?')
        urls.append(url)
        store_names_map[url] = {
            'store_name': store['store_name'],
            'name_on_platform': store['name_on_platform'],
            'location': store['location'],
            'platform': 'grabfood'
        }
    
    # Process Foodpanda stores
    foodpanda_data = store_data['cocopan_stores']['food_panda']
    
    # Main Foodpanda stores
    for store in foodpanda_data['main_list']:
        url = store['url']
        urls.append(url)
        store_names_map[url] = {
            'store_code': store['store_code'],
            'store_name': store['store_name'],
            'address': store['address'],
            'platform': 'foodpanda'
        }
    
    # Additional Foodpanda stores
    for store in foodpanda_data['additional_stores']:
        url = store['url']
        urls.append(url)
        store_names_map[url] = {
            'store_name': store['store_name'],
            'address': store['address'],
            'platform': 'foodpanda'
        }
    
    # Create branch_urls.json (expected by existing code)
    branch_urls = {
        "urls": urls,
        "meta": {
            "total_stores": len(urls),
            "grabfood_stores": len([u for u in urls if 'grab.com' in u]),
            "foodpanda_stores": len([u for u in urls if 'foodpanda' in u]),
            "generated_from": "cocopan_stores.json"
        }
    }
    
    # Create store_names.json (new file for name lookup)
    store_names = {
        "store_names": store_names_map,
        "meta": {
            "total_stores": len(store_names_map),
            "description": "URL to store name mapping for CocoPan stores"
        }
    }
    
    # Write the files
    with open('branch_urls.json', 'w') as f:
        json.dump(branch_urls, f, indent=2)
    
    with open('store_names.json', 'w') as f:
        json.dump(store_names, f, indent=2)
    
    # Print summary
    print(f"✅ Converted {len(urls)} stores:")
    print(f"   🛒 GrabFood: {len([u for u in urls if 'grab.com' in u])}")
    print(f"   🐼 Foodpanda: {len([u for u in urls if 'foodpanda' in u])}")
    print(f"✅ Created branch_urls.json")
    print(f"✅ Created store_names.json")
    
    return True

if __name__ == "__main__":
    # First, save your JSON data as 'cocopan_stores.json'
    print("🔄 Converting store data...")
    success = convert_store_data()
    if success:
        print("🎉 Conversion complete!")
    else:
        print("❌ Conversion failed!")