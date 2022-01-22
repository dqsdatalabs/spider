# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json 
import re

class MySpider(Spider):
    name = 'eplace_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    

    def start_requests(self):
        url = "https://eplacelive.search.windows.net/indexes/eplacelive-index/docs/search?api-version=2016-09-01"
        payload = "{\"filter\":\"listing_sale_or_rental eq 'Rental' and system_listing_state eq 'current' and feed_status ne 'stopped' and tags/all(t: t ne 'LaunchingSoon') and tags/all(t: t ne 'eplace-onhold') and is_deleted eq false\",\"orderby\":\"system_ctime desc\",\"top\":24,\"skip\":0,\"count\":true}"
        headers = {
            'authority': 'eplacelive.search.windows.net',
            'api-key': 'A1ADC2EEA6DE0F8388D60F480E55110C',
            'content-type': 'application/json',
            'accept': '*/*',
            'origin': 'https://www.eplace.com.au',
            'referer': 'https://www.eplace.com.au/rent/property-search',
            'accept-language': 'tr,en;q=0.9',
        }
        yield Request(url,
                    method="POST",
                    body=payload,
                    headers=headers,
                    callback=self.parse,
                    meta={"payload": payload, "headers": headers, "url": url})

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 24)
        seen = False

        data = json.loads(response.body)
        for item in data["value"]:
            seen = True
            property_type = item["subcategories"][0]
            follow_url = "https://www.eplace.com.au/property/" + item["address_state_or_region"].replace(" ", "-") + "-" + item["address_suburb_or_town"].replace(" ", "-") + "-" + item["default_external_id"]
            if get_p_type_string(property_type): yield Request(follow_url, callback=self.populate_item, meta={"property_type": get_p_type_string(property_type), "item":item})
        
        if page == 24 or seen:
            url = response.meta["url"]
            headers = response.meta["headers"]
            payload = response.meta["payload"].replace('skip":' + str(page - 24), 'skip":' + str(page))
            yield Request(url,
                    method="POST",
                    body=payload,
                    headers=headers,
                    callback=self.parse,
                    meta={"payload": payload, "headers": headers, "url": url, "page": page + 24})
            
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        data = response.meta["item"]
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_source", "Eplace_Com_PySpider_australia")
        external_id = data['property_id']
        if external_id:
            item_loader.add_value("external_id", external_id)

        address = data['address_formats_full_address']
        if address:
            item_loader.add_value("title", address)
            item_loader.add_value("address", address)

        city = data['address_suburb_or_town']
        if city:
            item_loader.add_value("city", city)

        zipcode_region = data['address_state_or_region']
        zipcode_post = data['address_postcode']
        if zipcode_post and zipcode_region :
            item_loader.add_value("zipcode", zipcode_region + " " + zipcode_post)

        price = data['price_match']
        if price:
            if price.isdigit():
                price = int(price)*4
                if price > 99999:                    
                    price = str(price).replace("000", "")                
                item_loader.add_value("rent", price)
        item_loader.add_value("currency", "AUD")

        desc = data['advert_internet_body']
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = data['attributes_bedrooms']
        if room_count:
            item_loader.add_value("room_count", room_count)
        
        bathroom_count = data['attributes_bathrooms']
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        parking = data['attributes_garages']
        if parking:
            item_loader.add_value("parking", True)

        for x in data['images']:
            image = x.split('"url":"')[1].split('"')[0]
            item_loader.add_value("images", response.urljoin(image))

        features = data['features']
        for i in features:
            if "balcony" in i.lower():
                item_loader.add_value("balcony", True)
            if "dishwasher" in i.lower():
                item_loader.add_value("dishwasher", True)
            if "pool" in i.lower():
                item_loader.add_value("swimming_pool", True)
            if "garage" in i.lower() or "parking" in i.lower():
                item_loader.add_value("parking", True)

        latitude = data['address_latitude']
        if latitude:    
            item_loader.add_value("latitude", latitude)

        longitude = data['address_longitude']
        if longitude:
            item_loader.add_value("longitude", longitude)

        landlord_name = data['listing_agent_1_name']
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)
        else:
            item_loader.add_value("landlord_name", "Place HQ")
        
        landlord_email = data['listing_agent_1_email_address']
        if landlord_email:
            item_loader.add_value("landlord_email", landlord_email)

        landlord_phone = data['listing_agent_1_phone_mobile']
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)
        phonecheck=response.url
        if phonecheck and "3428035" in phonecheck:
            item_loader.add_value("landlord_phone","07 3264 2311")


        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    else:
        return None