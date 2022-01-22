# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider 
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from urllib.parse import urljoin
import re
from datetime import datetime
import time

class MySpider(Spider):
    name = 'rentberry_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en' 

    def start_requests(self):

        # url = "https://rentberry.com/gb/houses/18955315-one-br-5-weymouth-mews-london-w1g-7dx-united-kingdom"
        # yield Request(url,callback=self.populate_item,meta={'property_type': "apartment"})
        start_urls = [
            {
                "url" : "https://rentberry.com/houses/s/london-uk",
                "property_type" : "house"
            },
            {
                "url" : "https://rentberry.com/apartments/s/london-uk",
                "property_type" : "apartment"
            },
             {
                "url" : "https://rentberry.com/condos/s/london-uk",
                "property_type" : "house"
            },
             {
                "url" : "https://rentberry.com/duplexes/s/london-uk",
                "property_type" : "house"
            },
             {
                "url" : "https://rentberry.com/townhouses/s/london-uk",
                "property_type" : "apartment"
            },
             {
                "url" : "https://rentberry.com/lofts/slondon-uk",
                "property_type" : "apartment"
            },
             {
                "url" : "https://rentberry.com/rooms/s/london-uk",
                "property_type" : "room"
            },
        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})
    


    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='apartment-item ng-star-inserted']/a/@href").extract():
            f_url = response.urljoin(item)            
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type"), "url":f_url},
            )
        
    # 2. SCRAPING level 2 
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Rentberry_PySpider_"+ self.country + "_" + self.locale)

        bathroom_count = response.xpath("//span[@class='icon bath-icon']/following-sibling::span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split(".")[0].strip())

        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        dontallow=response.xpath("//span[.='Rented']/text()").get()
        if dontallow and "rented" in dontallow.lower():
            return


        row_data = ""
        js = response.xpath("//script[contains(.,'SAVED_HTTP_STATE')]/text()").get()
        if js:
            js = js.split("STATE = ")[1].strip(";").strip()
            raw_data = json.loads(js)
         


            main_key = ''
            for key in raw_data:
                main_key = key
                break
                
        # data = raw_data["apartment/18955315/view?groups=SinglePicture,NewAmenitiesFormat"]["body"]["body"]["listing"]
        # a =data['address']['latitude']
        # print("----------",a)
        try:
            for i in raw_data:
                data = raw_data[f"{i}"]["body"]["body"]['listing']
                if data['address']:
                    latitude = data['address']['latitude']
                    if latitude:
                        item_loader.add_value("latitude", str(latitude))

                longitude = data['address']['longitude']
                if longitude:
                    item_loader.add_value("longitude", str(longitude))

                address = data['address']['formattedAddress']
                if address:
                    item_loader.add_value("address", address)

                zipcode = data['address']['zip']
                if zipcode:
                    item_loader.add_value("zipcode", str(zipcode))
                zipcodecheck=item_loader.get_output_value("zipcode")
                if not zipcodecheck:
                    zipcode1=data['address']['formattedAddress']
                    zipcode1=re.search("[A-Z]+[A-Z0-9]",zipcode1)
                    if zipcode1:
                        item_loader.add_value("zipcode", zipcode1.group())


                city = data['address']['city']
                if city:
                    item_loader.add_value("city", city)



                if response.xpath("//span[contains(@class,'sqft-icon')]").get():
                        square_meters = response.xpath("//span[@class='icon sqft-icon']/following-sibling::span/text()").get()
                        if square_meters:
                            item_loader.add_value("square_meters", str(int(float(square_meters) * 0.09290304)))
                else:
                    square_meters = data['space']
                    unit = data['lengthUnit']
                    if square_meters:
                        if unit.strip().startswith('m'):
                            item_loader.add_value("square_meters", str(square_meters))
                        
                        else:
                            item_loader.add_value("square_meters", str(int(float(square_meters) * 0.09290304)))

                    else:
                        square_meters = description = data['description']
                        if square_meters:
                            unit_pattern = re.findall(r"[+-]? *((?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)\s*(Sq. Ft.|sq. ft.|Sq. ft.|sq. Ft.|sq|Sq)",square_meters.replace(",",""))
                            if unit_pattern:
                                square_title=unit_pattern[0][0]
                                sqm = str(int(float(square_title) * 0.09290304))
                                item_loader.add_value("square_meters", sqm)

                if response.xpath("//span[contains(@class,'bed-icon')]").get():
                    room_count = response.xpath("//span[@class='icon bed-icon']/following-sibling::span/text()").get()
                    if room_count:
                        item_loader.add_value("room_count", room_count.strip())
                elif data['bedrooms']:
                    room_count = data['bedrooms']
                    if room_count:
                        item_loader.add_value("room_count", str(int(float(room_count))))
                
                rent = data['price']                
                if rent:
                    item_loader.add_value("rent", str(int(float(str(rent).replace(',', '.')))))
                    item_loader.add_value("currency", "GBP")
                
                external_id = data['id']
                if external_id:
                    item_loader.add_value("external_id", str(external_id))

                description = data['description']
                if description:
                    item_loader.add_value("description", description)

                images = []
                for i in data['apartmentPictures']:
                    images.append(i['imageThumbs']['apartment_1920'])
                external_images_count = str(len(images))
                if images:
                    item_loader.add_value("images", images)
                    item_loader.add_value("external_images_count", external_images_count)

                deposit = data['deposit']
                if deposit:
                    item_loader.add_value("deposit", str(deposit))

                utilities = data['utilitiesPrice']
                if utilities:
                    item_loader.add_value("utilities", str(utilities))

                pets_allowed1 = data['cat']
                pets_allowed2 = data['dog']
                if pets_allowed1 == True or pets_allowed2 == True:
                    item_loader.add_value("pets_allowed", True)

                amenities = data['amenities']
                if 'furnished' in amenities:
                    item_loader.add_value("furnished", True)
                if 'parking' in amenities:
                    item_loader.add_value("parking", True)
                if 'elevator' in amenities:
                    item_loader.add_value("elevator", True)
                if 'balcony' in amenities:
                    item_loader.add_value("balcony", True)
                if 'terrace' in amenities:
                    item_loader.add_value("terrace", True)
                if 'pool' in amenities:
                    item_loader.add_value("swimming_pool", True)
                if 'washerUnit' in amenities:
                    item_loader.add_value("washing_machine", True)
                if 'dishwasher' in amenities:
                    item_loader.add_value("dishwasher", True)

                floor = data['floors']
                if floor:
                    item_loader.add_value("floor", str(floor))

                if data['user']:
                    landlord_name1 = data['user']['nameFirst']
                    landlord_name2 = data['user']['nameMiddle']
                    landlord_name3 = data['user']['nameLast']
                    landlord_name = ''
                    if landlord_name1:
                        landlord_name += landlord_name1
                    if landlord_name2:
                        landlord_name += ' ' + landlord_name2
                    if landlord_name3:
                        landlord_name += ' ' + landlord_name3
                    if landlord_name:
                        item_loader.add_value("landlord_name", landlord_name)

                    landlord_phone = data['user']['phone']
                    if landlord_phone:
                        item_loader.add_value("landlord_phone", str(landlord_phone))
                    
                    landlord_email = data['user']['username']
                    if landlord_email:
                        item_loader.add_value("landlord_email", landlord_email)
                   
                if not item_loader.get_collected_values("terrace"):
                    if response.xpath("//h1/text()[contains(.,'Terrace') or contains(.,'terrace')]").get(): item_loader.add_value("terrace", True)
                
                if not item_loader.get_collected_values("furnished"):
                    if '-' in description and 'unfurnished' in description.lower(): item_loader.add_value("furnished", False)
                    elif '-' in description and 'furnished' in description.lower(): item_loader.add_value("furnished", True)
                
                if not item_loader.get_collected_values("parking"):
                    if '-' in description and 'parking' in description.lower(): item_loader.add_value("parking", True)

                if not item_loader.get_collected_values("city"):
                    if response.xpath("//span[@class='address']/text()").get(): item_loader.add_value("city", response.xpath("//span[@class='address']/text()").get().split(',')[0].strip())

                if not item_loader.get_collected_values("landlord_name"): item_loader.add_value("landlord_name", 'Rentberry')
                if not item_loader.get_collected_values("landlord_phone"): item_loader.add_value("landlord_phone", '+14157957171')
                if not item_loader.get_collected_values("landlord_email"): item_loader.add_value("landlord_email", 'support@rentberry.com')

                yield item_loader.load_item()

        except:
            pass