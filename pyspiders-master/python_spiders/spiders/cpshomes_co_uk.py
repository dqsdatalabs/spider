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
from datetime import datetime
class MySpider(Spider):
    name = 'cpshomes_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    thousand_separator = ','
    scale_separator = '.'
    def start_requests(self):

        start_urls = [
            {
                "url" : "https://www.cpshomes.co.uk/renting/properties/all?tp=Apartment",
                "property_type" : "apartment"
            },
            {
                "url" : "https://www.cpshomes.co.uk/renting/properties/all?tp=House",
                "property_type" : "house"
            },
            {
                "url" : "https://www.cpshomes.co.uk/renting/properties/all?tp=House%20-%20Room%20Only",
                "property_type" : "room"
            },
        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)

        script_data = response.xpath("//script[contains(.,'ldpc.init')]/text()").get()
        data = json.loads(script_data.split("ldpc.init(")[1].strip(")").strip())

        seen = False
        for item in data["List"]:
            area = item["Area"]
            ref = item["Reference"]
            f_url = f"https://www.cpshomes.co.uk/renting/properties/{area}/{ref}"
            item_dict = {
                "lat" : str(item["Latitude"]),
                "lng" : str(item["Longitude"]),
                "price" : str(item["Price"]),
                "room_count" : str(item["Bedrooms"]),
                "bathroom_count" : str(item["Bathrooms"]),
                "property_type" : response.meta.get("property_type"),
            }
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta=item_dict,
            )
            seen = True
        
        if page == 2 or seen:
            p_url = response.url.split("&p=")[0] + f"&p={page}"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1, 'property_type': response.meta.get('property_type')}
            )
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        if response.meta.get('property_type'):
            item_loader.add_value("property_type", response.meta.get('property_type'))
            item_loader.add_value("external_link", response.url)

            item_loader.add_value("latitude", response.meta.get('lat'))
            item_loader.add_value("longitude", response.meta.get('lng'))
            item_loader.add_value("room_count", response.meta.get('room_count'))
            item_loader.add_value("bathroom_count", response.meta.get('bathroom_count'))
            item_loader.add_value("external_source", "Cpshomes_PySpider_"+ self.country + "_" + self.locale)
            rent = response.meta.get('price')
            if rent:
                item_loader.add_value("rent", rent)
                item_loader.add_value("currency", "GBP")

            deposit = response.xpath("//div[@class='dep']//span//text()[not(contains(.,'Zero'))]").extract_first()
            if deposit:
                item_loader.add_value("deposit", deposit.split("£")[1].replace(",",""))

            

            json_value = response.xpath("//script[@type='text/javascript']//text()[contains(.,'ldpc.init')]").extract_first()
            if json_value:
                json_add = json_value.split("ldpc.propertySuccess(")[1].replace("})","}")
                data = json.loads(json_add)
            
                item_loader.add_value("title", str(data["Title"]))
                
                images = [response.urljoin(x) for x in data["Photos"]]
                item_loader.add_value("images", images)

                floor_plan_images = [response.urljoin(x) for x in data["Floorplans"]]
                item_loader.add_value("floor_plan_images", floor_plan_images)

                item_loader.add_value("external_id",str(data["Reference"]))
                item_loader.add_value("zipcode",str(data["Postcode"]))

                address = data["Street"] +", "+ data["Area"] 
                if address:
                    item_loader.add_value("address", address)
                city = data["Area"]
                if city:
                    item_loader.add_value("city",city)
                deposit = data["Deposit"] 
                if deposit !=0:
                    item_loader.add_value("deposit", str(deposit))

                furnished = data["Furnishing"] 
                if furnished=="Unfurnished":
                    item_loader.add_value("furnished", False)
                if furnished=="Furnished":
                    item_loader.add_value("furnished", True)

                a_date = data["Available"]
                if a_date:
                    a_date = a_date.split(" ")[0]
                    datetimeobject = datetime.strptime(a_date,'%d/%m/%Y')
                    newformat = datetimeobject.strftime('%Y-%m-%d')
                    item_loader.add_value("available_date", newformat)
            
            desc = "".join(response.xpath("//div[@class='collapser-body']//p//text()").extract())
            if desc:
                item_loader.add_value("description", desc.strip())
            
            parking = response.xpath("//script//text()[contains(.,'parking')]").get()
            if parking: 
                item_loader.add_value("parking", True)
            
            item_loader.add_value("landlord_name", "CPS Homes")

            item_loader.add_value("landlord_email", "enquiries@cpshomes.co.uk")
            
            item_loader.add_value("landlord_phone", "029 2066 8585")
            
            yield item_loader.load_item()

