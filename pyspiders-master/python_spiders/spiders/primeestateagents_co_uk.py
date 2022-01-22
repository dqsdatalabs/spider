# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'primeestateagents_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en' 
    
    def start_requests(self):
        formdata = {
            "data[property-type][slug]": "property-type",
            "data[property-type][baseSlug]": "property_type",
            "data[property-type][key]": "property-type",
            "data[property-type][units]": "",
            "data[property-type][compare]": "=",
            "data[property-type][values][0][name]": "Residential",
            "data[property-type][values][0][value]": "residential",
            "data[offer-type][compare]": "=",
            "data[offer-type][key]": "offer-type",
            "data[offer-type][slug]": "offer-type",
            "data[offer-type][values][0][name]": "For Rent",
            "data[offer-type][values][0][value]": "rent",
            "page": "1",
            "limit": "6",
            "sortBy": "newest",
            "currency": "any",
        }
        url = "http://www.primeestateagents.co.uk/wp-json/myhome/v1/estates?currency=any"
        yield FormRequest(
            url,
            callback=self.parse,
            formdata=formdata,
        )

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False

        data = json.loads(response.body)
        for item in data["results"]:
            follow_url = item["link"]
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            formdata = {
                "data[property-type][slug]": "property-type",
                "data[property-type][baseSlug]": "property_type",
                "data[property-type][key]": "property-type",
                "data[property-type][units]": "",
                "data[property-type][compare]": "=",
                "data[property-type][values][0][name]": "Residential",
                "data[property-type][values][0][value]": "residential",
                "data[offer-type][compare]": "=",
                "data[offer-type][key]": "offer-type",
                "data[offer-type][slug]": "offer-type",
                "data[offer-type][values][0][name]": "For Rent",
                "data[offer-type][values][0][value]": "rent",
                "page": str(page),
                "limit": "6",
                "sortBy": "newest",
                "currency": "any",
            }
            url = "http://www.primeestateagents.co.uk/wp-json/myhome/v1/estates?currency=any"
            yield FormRequest(
                url,
                callback=self.parse,
                formdata=formdata,
                meta={"page":page+1}
            )
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)

        f_text = " ".join(response.xpath("//h1//text()").getall())
        prop_type = ""
        if get_p_type_string(f_text):
            prop_type = get_p_type_string(f_text)
        else:
            f_text = " ".join(response.xpath("//div[contains(@class,'description')]//text()").getall())
            if get_p_type_string(f_text):
                prop_type = get_p_type_string(f_text)
            else:
                return
        item_loader.add_value("external_source", "Primeestateagents_Co_PySpider_united_kingdom")
        item_loader.add_xpath("title", "//div/h1[@class='mh-top-title__heading']/text()")
        zipcode = " ".join(response.xpath("//li[contains(.,'Post Code')]/a/text()").extract()) 
        address_title = response.xpath("//div/h1[@class='mh-top-title__heading']/text()").extract_first()
        if address_title:
            if "furnished" in address_title:
                item_loader.add_value("furnished", True)
            address = ""
            if " in " in address_title:
                address = address_title.split(" in ")[-1].strip()
                if "." in address:
                    address = address.split(".")[0].strip()
            elif " at " in address_title:
                address = address_title.split(" at ")[-1].strip()
                if "." in address:
                    address = address.split(".")[0].strip()
            if address:
                item_loader.add_value("address", address)
                address1 = address.replace(",","").replace("/","").split(" ")
                postcode = ""
                for i in address1:
                    if not i.isalpha() and " " not in i:
                        postcode = i
                        
                if zipcode:
                    item_loader.add_value("zipcode", zipcode.strip())
                    if postcode:
                        city = address.split(postcode)[0].replace(",","").strip().split(" ")[-1]
                        item_loader.add_value("city", city)
                elif postcode:
                    item_loader.add_value("zipcode", postcode)
                    city = address.split(postcode)[0].replace(",","").strip().split(" ")[-1]
                    if city == "End":
                        city = address.split(postcode)[0].split("/")[1].replace(",","").strip()
                    item_loader.add_value("city", city)
            else:
                if " to " in address_title:
                    address = address_title.split(" to ")[1].strip()
                elif " near " in address_title:
                    address = address_title.split(" near ")[1].strip()
                
                if address:
                    address = address.replace("area!","").strip()
                    item_loader.add_value("address", address)
                    
                    if "," in address:
                        if zipcode:
                            item_loader.add_value("zipcode", zipcode.strip())
                        else:
                            item_loader.add_value("zipcode", address.split(",")[-1].strip())
                        item_loader.add_value("city", address.split(",")[-2].strip())
                    else:
                        if zipcode:
                            item_loader.add_value("zipcode", zipcode.strip())
                        else:
                            item_loader.add_value("zipcode", address.split(" ")[-1])
                        item_loader.add_value("city", address.split(" ")[0])
                
        rent = " ".join(response.xpath("//div[@class='mh-estate__details__price__single']//text()").extract())
        if rent:
            if "week" in rent.lower():
                rent = rent.split('£')[-1].split('/')[0].strip().replace(',', '').replace('\xa0', '')
                item_loader.add_value("rent", str(int(float(rent)) * 4))                
            else:
                rent = rent.split('£')[-1].split('/')[0].strip().replace(',', '').replace('\xa0', '')
                item_loader.add_value("rent", str(int(float(rent))))
        item_loader.add_value("currency", 'GBP')

        external_id = " ".join(response.xpath("//li[span[contains(.,'ID')]]/text()").extract())
        if external_id:   
            item_loader.add_value("external_id",external_id.strip())    
        
        room_count = " ".join(response.xpath("//li[strong[contains(.,'Bedroom')]]/text()").extract())
        if room_count and room_count.strip() == "Room":
            item_loader.add_value("room_count","1")
        elif room_count:   
            item_loader.add_value("room_count",room_count)     
            
        if room_count and "Room" in room_count:
            item_loader.add_value("property_type", "room")
        else:
            item_loader.add_value("property_type", prop_type)
            
        bathroom_count = " ".join(response.xpath("//li[strong[contains(.,'Bathroom')]]/text()").extract())
        if bathroom_count:   
            item_loader.add_value("bathroom_count",int(float(bathroom_count.strip())))      
              
        furnished =response.xpath("//ul//li[contains(.,'Furnished') or contains(.,' furnished') and not(contains(.,'Un'))]//text()").extract_first()    
        if furnished:
            item_loader.add_value("furnished", True)
            
        balcony =response.xpath("//ul/li[contains(.,'Balcony')]//text()").extract_first()    
        if balcony:
            item_loader.add_value("balcony", True)
        images = [x for x in response.xpath("//div[@id='mh_rev_gallery_single']/ul/li/img/@src").extract()]
        if images:
                item_loader.add_value("images", images)
        else:
            images = [x for x in response.xpath("//div[@class='mh-estate__main-image']/a/img/@src").extract()]
            if images:
                    item_loader.add_value("images", images)
            
        parking =response.xpath("//ul/li[contains(.,'parking')]/text()").extract_first()    
        if parking:
            item_loader.add_value("parking", True)
    
        desc = " ".join(response.xpath("//div[contains(@class,'mh-estate__section--description')]//text()[.!='Details']").extract())
        if desc:
            item_loader.add_value("description", desc.replace("Share on Facebook   Tweet   Pin it   LinkedIn","").strip())

        item_loader.add_value("landlord_name", "Prime Estate Agents")
        item_loader.add_value("landlord_phone", "02073751188")
        item_loader.add_value("landlord_email", "info@primeestateagents.co.uk")   
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "etage" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "woning" in p_type_string.lower() or "huis" in p_type_string.lower()):
        return "house"
    else:
        return None