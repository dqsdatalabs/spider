# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from inspect import isframe
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'doorsteps_com_disabled'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "Doorsteps_PySpider_italy"
    start_urls = ['https://www.doorsteps.com/']  # LEVEL 1

    def start_requests(self):
        
        yield Request( 
            url=self.start_urls[0],
            callback=self.jump,
        )
    
    def jump(self, response):
        for loc in response.xpath("//h3[contains(.,'Rental Cities')]/following-sibling::ul//a"):
            city = loc.xpath(".//text()").get().replace("Apartments", "").strip()
            yield Request(response.urljoin(loc.xpath("./@href").get()), self.parse, meta={"city": city})

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//li[@class='srp-list__item']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": "apartment", "city": response.meta.get('city')})
            seen = True
        
        next_page = response.xpath("//a[contains(@class,'right')]/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse, meta={"city": response.meta.get('city')})

    # 2. SCRAPING level 2
    def populate_item(self, response): 
        
        data_place = response.xpath("//script[@id='__NEXT_DATA__']/text()").get()
        data_dict = json.loads(data_place)
        external_id = (response.url).split("/")[-1].split("-")[0]
        
        data = data_dict["props"]["initialState"]["listings"][f"{external_id}"]["floorPlans"]
        features = data_dict["props"]["initialState"]["listings"][f"{external_id}"].get("community")
        if features:
            features=features.get("features")
         

        for num,house in enumerate(data):
            item_loader = ListingLoader(response=response)
            item_loader.add_value("external_id",external_id)

            if house.get("beds"):
                item_loader.add_value("room_count",house.get("beds"))
            if house.get("price"):
                item_loader.add_value("rent",house.get("price"))
            if house.get("sqft"):
                meters = int(int(house.get("sqft")) // 10.764)
                item_loader.add_value("square_meters",meters)
            if house.get("baths"):
                item_loader.add_value("bathroom_count",int(house.get("baths")))
            
            if house.get("photo"):
                item_loader.add_value("images",house.get("photo"))
            if features:
                if "Balcony" in features:
                    item_loader.add_value("balcony",True)

                if "Parking" in features:
                    item_loader.add_value("parking",True)

            rentcheck=item_loader.get_output_value("rent")
            if not rentcheck:
                rent=response.xpath("//h1[@class='listing-header__title']/text()").get()
                if rent:
                    rent=rent.split("–")[0].split("$")[1].strip()
                    item_loader.add_value("rent",rent)

            

            available_date = house.get("availability").get("date")
            if available_date:
                item_loader.add_value("available_date", house.get("availability").get("date"))
            else:
                item_loader.add_value("available_date", "2021-10-31")
            


            item_loader.add_value("external_link", f"{response.url}#{num}")
            

            item_loader.add_value("property_type", response.meta.get('property_type'))
            item_loader.add_value("external_source", self.external_source)
            title=" ".join(response.xpath("//li[@class='breadcrumbs__item']/a/text()").getall())
            if title:
                item_loader.add_value("title",title)
            adres=response.xpath("//div[@class='search__section']/p/text()").get()
            if adres:
                item_loader.add_value("address",adres)
            else:
                address = response.xpath("//a[@data-attr='breadcrumb-1']/text()").get()
                if address:
                    item_loader.add_value("address",address)
            city = response.meta.get("city")
            item_loader.add_value("city", city)

            
            zipcode_x=response.xpath("//div[@class='search__section']/p/text()").get()
            if zipcode_x:
                item_loader.add_value("zipcode",zipcode_x.split(",")[-1])
            else:
                addition = (response.url).split("-")[-2]
                zipcode = response.xpath("//a[@data-attr='breadcrumb-2']/text()").get()
                if zipcode.isdigit():
                    item_loader.add_value("zipcode",addition.upper() + " " + zipcode)
                else:
                    zipcode = response.xpath("//a[@data-attr='breadcrumb-3']/text()").get()
                    if zipcode.isdigit():
                        item_loader.add_value("zipcode",addition.upper() + " " + zipcode)
                    else:
                        zipcode = response.xpath("//a[@data-attr='breadcrumb-4']/text()").get()
                        item_loader.add_value("zipcode",addition.upper() + " " + zipcode)                   
            
            item_loader.add_value("currency","USD")

            desc=response.xpath("//p[@data-attr='description-text']/text()").get()
            if desc:
                item_loader.add_value("description",desc)
            latitude=response.xpath("//script[@id='__NEXT_DATA__']/text()").get()
            if latitude:
                latitude=latitude.split("lng")[0].split("lat")[-1].split(",")[0].replace('":',"")
                item_loader.add_value("latitude",latitude)
            longitude=response.xpath("//script[@id='__NEXT_DATA__']/text()").get()
            if longitude:
                longitude=longitude.split("lng")[1].split(",")[0].replace('":',"")
                item_loader.add_value("longitude",longitude)

            
            phones = response.xpath("//p[@class='listing-info__text']/text()").getall()
            if phones:
                if len(phones)>2:
                    # landlord_name=phones[0]
                    # if landlord_name and "city" in landlord_name.lower() and "apartments" in landlord_name.lower() and "cortland" in landlord_name.lower() and "Flats" in landlord_name.lower() and "plaza" in landlord_name.lower() and "american" in landlord_name.lower():
                    # if landlord_name :
                    item_loader.add_value("landlord_name","Door Steps")
                    # else:
                    #     item_loader.add_value("landlord_name",landlord_name)

                    item_loader.add_value("landlord_phone",phones[1])

            item_loader.add_value("landlord_email","support@doorsteps.com")



            yield item_loader.load_item()




            

        
        


        