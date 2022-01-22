# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import json
from datetime import datetime
import scrapy
from scrapy import Request
from ..loaders import ListingLoader


class RentbunkSpider(scrapy.Spider):
    name = 'rentbunk_com'
    allowed_domains = ['app.rentbunk.com', '0ngcvtiqsm-dsn.algolia.net']
    start_urls = ['https://app.rentbunk.com/']
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    position = 0
    external_source = "Rentbunk_PySpider_united_kingdom_en"

    def start_requests(self):
        url = "https://0ngcvtiqsm-dsn.algolia.net/1/indexes/prod_PROPERTIES-listed/query?x-algolia-agent=Algolia%20for%20JavaScript%20(3.35.1)%3B%20Browser%20(lite)&x-algolia-application-id=0NGCVTIQSM&x-algolia-api-key=f9ece56187569c40e8179ab6a698d0e5"
        params = {
            "params": "hitsPerPage=9&filters=is_listed%3D1+AND+%28number_bedrooms%3A0+TO+100%29+AND+%28tenant_preference%3Aeither+OR+tenant_preference%3Aprofessional+OR+tenant_preference%3Astudent%29+AND+%28household_rent_pcm+%3E%3D+0%29+OR+%28hmo_min_pcm+%3E%3D+0%29+OR+%28hmo_max_pcm+%3E%3D+0%29&page=0&aroundRadius=1000"
        }
        yield Request(url=url, 
                      callback=self.parse,
                      method="POST", 
                      body=json.dumps(params),
                      meta={'page': 0,
                            'response_url': url})

    def parse(self, response, **kwargs):

        total_items_data = json.loads(response.body.decode("utf-8"))
        for item in total_items_data['hits']:
            item_loader = ListingLoader(response=response)
            floor_plan = ""
            epc = ""
            if "property_documents" in item:
                property_document = item['property_documents']
                available_variable = (list(item['bedrooms']))[0]
                floor_plan = next((i for i, item in enumerate(property_document) if item["name"] == "Floorplan"), None)
                epc = next((i for i, item in enumerate(property_document) if item["name"] == "EPC"), None)
            if floor_plan:
                item_loader.add_value('floor_plan_images', item['property_documents'][floor_plan]['image_url'])
            images = ""
            if "image_large_url" in item:
                images = [i['image_large_url'] for i in item['property_photos']]
                item_loader.add_value('images', images)
            if not item_loader.get_collected_values("images"):
                images = [item['property_photo']]
                item_loader.add_value('images', images)

            property_type = item['property_type']
            apartment_types = ["flat", 'maisonette', 'apartment']
            house_types = ['house', 'terraced', 'semi-detached', 'detached', 'home', 'bungalow']
            studio_types = ["studio"]
            if property_type.lower() in apartment_types:
                property_type = "apartment"
            elif property_type.lower() in house_types:
                property_type = "house"
            elif property_type.lower() in studio_types:
                property_type = "studio"
            else: continue

            

            item_loader.add_value('property_type', property_type)
            item_loader.add_value('external_link', f"https://app.rentbunk.com/rent-property/{item['property_id']}")
            item_loader.add_value('external_id', item["property_id"])
            item_loader.add_value('title', f"{item['number_bedrooms']} bedroom {property_type}")

            item_loader.add_value('address', item['address']['first_line_address'])
            if 'second_line_address' in item['address']:
                if item['address']['second_line_address']:
                    item_loader.add_value('address', item['address']['second_line_address'])
        

            item_loader.add_value('latitude', str(item['address']['lat']))
            item_loader.add_value('longitude', str(item['address']['lng']))
            if "description" in item:
                item_loader.add_value('description', item['description'])
            
            item_loader.add_value('bathroom_count', str(item['number_bathrooms']))
            item_loader.add_value('room_count', item['number_bedrooms'])
            item_loader.add_value('landlord_name', 'rentbunk')
            item_loader.add_value('landlord_phone', '0117 442 0533')
            item_loader.add_value('landlord_email', 'info@rentbunk.com')
            item_loader.add_value('zipcode', item['address']['post_code'])
            item_loader.add_value('city', item['address']['city'])
            if 'deposit_amount' in item:
                item_loader.add_value('deposit', item['deposit_amount'])

            # ex https://www.drewloholdings.com/apartments-for-rent/south-carriage-place
            if 'household_rent_pcm' in item:
                item_loader.add_value('rent_string', f"£{item['household_rent_pcm']}")
            else:
                item_loader.add_value('rent_string', f"£{item['hmo_max_pcm']}")

            
            if epc:
                for epc in item['property_documents']:
                    epc = epc.get("epc_rating")
                    if epc is not None:
                        item_loader.add_value('energy_label', epc)

            # ex https://app.rentbunk.com/rent-property/tpgOMaG9E0Outn6kdUZC
            if "parking_access" in item:
                if item['parking_access'] != 'no parking':
                    item_loader.add_value('parking', True)
            if "custom_amenities" in item:
                if "Bike Storage" in item["custom_amenities"] and not item_loader.get_collected_values("parking"): item_loader.add_value("parking", True)

            # ex https://app.rentbunk.com/rent-property/tpgOMaG9E0Outn6kdUZC
            if 'terrace' in item['property_type'] or 'terrace' in item_loader.get_output_value('description').lower():
                item_loader.add_value('terrace', True)
            if "default_amenities" in item:
            # ex https://app.rentbunk.com/rent-property/IxgAi5haEnHxRzlgvq5p present in description
                if item['default_amenities']['fully_furnished'] or item['default_amenities']['partly_furnished']:
                    item_loader.add_value('furnished', True)
                elif item["default_amenities"]["unfurnished"]:
                    item_loader.add_value('furnished', False)

                if item['default_amenities']['washer']:
                    item_loader.add_value('washing_machine', True)

            # ex https://app.rentbunk.com/rent-property/YWKwV6CpBG51ZeBgwJqO present in description
            if 'balcony' in item_loader.get_output_value('description').lower():
                item_loader.add_value('balcony', True)
      
            if 'date_available' in item.keys() and item['date_available']:
                available_date = datetime.fromtimestamp(item['date_available']['_seconds']).strftime("%d-%m-%Y")
                item_loader.add_value('available_date', available_date)

            self.position += 1
            item_loader.add_value('position', self.position)
            item_loader.add_value("external_source", self.external_source)
            yield item_loader.load_item()
        if len(total_items_data['hits']) > 0:
            params = {
                    "params": f"hitsPerPage=9&filters=is_listed%3D1+AND+%28number_bedrooms%3A0+TO+100%29+AND+%28tenant_preference%3Aeither+OR+tenant_preference%3Aprofessional+OR+tenant_preference%3Astudent%29+AND+%28household_rent_pcm+%3E%3D+0%29+OR+%28hmo_min_pcm+%3E%3D+0%29+OR+%28hmo_max_pcm+%3E%3D+0%29&page={response.meta.get('page')+1}&aroundRadius=1000"
                }
            yield Request(url=response.meta.get('response_url'), 
                          callback=self.parse,
                          method="POST", 
                          body=json.dumps(params),
                          meta={'page': response.meta.get('page')+1,
                                'response_url': response.meta.get('response_url')})
