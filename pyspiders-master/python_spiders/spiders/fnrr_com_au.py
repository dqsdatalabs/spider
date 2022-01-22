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
    name = 'fnrr_com_au'
    headers = {
        "accept": "*/*",
        "referer": "https://fnrr.com.au/lease",
        "x-requested-with": "XMLHttpRequest"
    }
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://fnrr.com.au/json/data/listings/?authenticityToken=qNf5h3FK8SDtj2uMkegjqA%3D%3D&_method=post&input_types=&office_id=&listing_category=&staff_id=&postcode=&rental_features=&listing_sale_method=Lease&rental_features=&status=&listing_suburb_search_string=&listing_suburb_id=&surrounding_radius=6&listing_property_type=Apartment&listing_property_type=Flat&listing_property_type=Unit&LISTING_BEDROOMS=&LISTING_BATHROOMS=&CARPORTS=&LISTING_PRICE_FROM=&LISTING_PRICE_TO=&sort=date-desc&gallery&limit=12",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://fnrr.com.au/json/data/listings/?authenticityToken=qNf5h3FK8SDtj2uMkegjqA%3D%3D&_method=post&input_types=&office_id=&listing_category=&staff_id=&postcode=&rental_features=&listing_sale_method=Lease&rental_features=&status=&listing_suburb_search_string=&listing_suburb_id=&surrounding_radius=6&listing_property_type=House&listing_property_type=Townhouse&LISTING_BEDROOMS=&LISTING_BATHROOMS=&CARPORTS=&LISTING_PRICE_FROM=&LISTING_PRICE_TO=&sort=date-desc&gallery&limit=12"
                ],
                "property_type": "house"
            },
            {
                "url": [
                    "https://fnrr.com.au/json/data/listings/?authenticityToken=qNf5h3FK8SDtj2uMkegjqA%3D%3D&_method=post&input_types=&office_id=&listing_category=&staff_id=&postcode=&rental_features=&listing_sale_method=Lease&rental_features=&status=&listing_suburb_search_string=&listing_suburb_id=&surrounding_radius=6&listing_property_type=Retail&LISTING_BEDROOMS=&LISTING_BATHROOMS=&CARPORTS=&LISTING_PRICE_FROM=&LISTING_PRICE_TO=&sort=date-desc&gallery&limit=12"
                ],
                "property_type": "studio"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    headers=self.headers,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        seen=False
        data = json.loads(response.body)
        if data["data"]["total"] >0:            
            for item in data["data"]["listings"]:
                url = item["listing_url"]
                yield Request(response.urljoin(url), callback=self.populate_item, meta={"item":item,"property_type":response.meta.get('property_type')})
                seen=True
        
        if page ==2 or seen:      
            f_url = response.url.split("&pg=")[0]+f"&pg={page}"
            yield Request(f_url, callback=self.parse,headers=self.headers, meta={"page": page+1, 'property_type':response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item = response.meta.get('item')
        type_ = item['listing_category']
        if "Commercial" == type_:
            return

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Fnrr_PySpider_australia")
        
        external_id = item['listing_id']
        if external_id:
            item_loader.add_value("external_id", str(external_id))

        title = item['listing_full_address']
        if title:
            item_loader.add_value("title", title)

        address = item['listing_full_address']
        if address:
            item_loader.add_value("address", address.strip())

        city = item['locality']
        if city:
            item_loader.add_value("city", city.strip())

        zipcode = item['postcode']
        zipcode_ = item['state']
        if zipcode and zipcode_:
            item_loader.add_value("zipcode", zipcode_+" "+zipcode)

        rent = item['rentals_rent_pw']
        if rent:
            rent = int(rent)*4
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "AUD")

        deposit = response.xpath("//div[contains(@class,'text-feat')]//text()[contains(.,'Bond')]").get()
        if deposit:
            deposit = deposit.split("$")[1].strip().replace(",","")
            item_loader.add_value("deposit", deposit)

        desc = " ".join(response.xpath("//div[contains(@class,'description')]//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = item['listing_bedrooms']
        if room_count:
            item_loader.add_value("room_count", room_count)

        bathroom_count = item['listing_bathrooms']
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in item['listing_gallery']]
        if images:
            item_loader.add_value("images", images)
        
        from datetime import datetime
        import dateparser
        available_date = item['rentals_date_available']
        if available_date:
            available_date = available_date.split("00:")[0].replace(",","").strip()
            if not "now" in available_date.lower():
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        parking = item['listing_garages']
        if parking:
            item_loader.add_value("parking", True)

        pets_allowed = item['listing_pets']
        if pets_allowed and "yes" in pets_allowed.lower():
            item_loader.add_value("pets_allowed", True)
        
        latitude = item['latitude']
        if latitude:     
            item_loader.add_value("latitude", str(latitude))
        
        longitude = item['longitude']
        if longitude:           
            item_loader.add_value("longitude", str(longitude))

        landlord_name = response.xpath("//div[contains(@class,'pure')]//h4//strong//a//text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)
        else:
            item_loader.add_value("landlord_name", "FIRST NATIONAL REAL ESTATE | REGENCY REALTY")
        
        landlord_phone = response.xpath("//div[contains(@class,'pure')]//a[contains(@href,'tel')]//text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)
        else:        
            item_loader.add_value("landlord_phone", "02 9518 8888")
        item_loader.add_value("landlord_email", "frontdesk@pyrmontfn.com.au")
        
        yield item_loader.load_item()