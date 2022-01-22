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
    name = 'property_tyne2let_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
      
    def start_requests(self):
        start_urls = [
            {
                "url": ["https://property.tyne2let.co.uk/?id=41266&action=view&route=search&view=list&input=NE2&jengo_radius=200&jengo_property_for=2&jengo_category=5&jengo_property_type=-1&jengo_min_price=0&jengo_max_price=99999999999&jengo_min_beds=0&jengo_max_beds=9999&jengo_min_bathrooms=&jengo_max_bathrooms=9999&min_land=&max_land=&min_space=&max_space=&jengo_branch=&country=&daterange=&jengo_order=6&trueSearch=&searchType=postcode&latitude=&longitude=&pfor_complete=&pfor_offer=&"
                ],
 
            },
            {
                "url": [
                    "https://property.tyne2let.co.uk/?id=41266&action=view&route=search&view=list&input=NE2&jengo_property_for=2&jengo_category=1&jengo_radius=200&jengo_property_type=-1&jengo_min_beds=0&jengo_max_beds=9999&jengo_min_price=0&jengo_max_price=99999999999&jengo_order=6&pfor_complete=&pfor_offer=&trueSearch=&searchType=postcode&latitude=&longitude=#total-results-wrapper"
                ],
  
            }
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse
                )

    # 1. FOLLOWING 
    def parse(self, response):
        page = response.meta.get('page', 2)       
        seen = False
        for item in response.xpath("//div[@class='col-md-6 marg-b-20']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        if page==2 or seen:
            url = f"https://property.tyne2let.co.uk/?id=41266&action=view&route=search&view=list&input=NE2&jengo_radius=200&jengo_property_for=2&jengo_category=5&jengo_property_type=-1&jengo_min_price=0&jengo_max_price=99999999999&jengo_min_beds=0&jengo_max_beds=9999&jengo_min_bathrooms=&jengo_max_bathrooms=9999&min_land=&max_land=&min_space=&max_space=&jengo_branch=&country=&daterange=&jengo_order=6&trueSearch=&searchType=postcode&latitude=&longitude=&pfor_complete=&pfor_offer=&page={page}"
            yield Request(url, callback=self.parse, meta={"page": page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Tynetolet_PySpider_united_kingdom")
        proptype =response.xpath("//h2[@class='title-details']/following-sibling::span[2]/strong/text()").get()

        if get_p_type_string(proptype):
            item_loader.add_value("property_type", get_p_type_string(proptype))
        else:
            return  

        externalid=response.url
        if externalid:
            externalid=externalid.split("property/")[1]
            externalid=externalid.split("/")[0]
        item_loader.add_value("external_id", externalid)

        title =response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title)

        desc = "".join(response.xpath("//p[@class='description-text']/following-sibling::p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())          
            item_loader.add_value("description", desc)

        address ="".join(response.xpath("//div[@class='details-address-wrap']//span//text()").getall())
        if address:
            address=re.sub('\s{2,}',' ',address.strip())
            item_loader.add_value("address", address)
            city=address.split(",")[-2]
            item_loader.add_value("city", city.strip())
            zipcode=address.split(",")[-1]           
            zipcode=re.search("[A-ZA-Z]+[0-9]",zipcode)
            if zipcode:
               item_loader.add_value("zipcode", zipcode.group())
            zipcodecheck=item_loader.get_output_value("zipcode")
            if not zipcodecheck:
                zipcode=response.xpath("//title//text()").get()
                if zipcode:
                    zipcode=zipcode.split("-")[0].strip().split(" ")[-1]
                    if zipcode:
                        item_loader.add_value("zipcode",zipcode.upper())
            citycheck=item_loader.get_output_value("city")
            if not citycheck:
                city=response.xpath("//title//text()").get()
                if city:
                    city=city.split("-")[0].strip().split(" ")[-2]
                    
                    item_loader.add_value("city",city.strip())



        rent =response.xpath("//h2[@class='details-name pull-left']/following-sibling::a/text()").get()
        if rent:
            rent = rent.strip().split("£")[1].split(".")[0].replace(",","").strip()
            item_loader.add_value("rent", int(rent))
        item_loader.add_value("currency", "GBP")

        room_count =response.xpath("//span[.='Bedrooms']/parent::span/following-sibling::span/text()").get()
        if room_count:
            room_count =re.findall("\d",room_count)
            item_loader.add_value("room_count", room_count)

        bathroom_count =response.xpath("//span[.='Bathrooms']/following-sibling::span/text()").get()
        if bathroom_count:
            bathroom_count =re.findall("\d",bathroom_count)
            item_loader.add_value("bathroom_count", bathroom_count)
        images = [x for x in response.xpath("//div[contains(@class,'fotorama')]//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        parking =response.xpath("//span[.='Parking']/following-sibling::span/text()").get()
        if parking and "yes" in parking.lower():
            item_loader.add_value("parking", True)
        if parking and "no" in parking.lower():
            item_loader.add_value("parking", False)
            
        terrace = response.xpath("//span[.='Gardens']/following-sibling::span/text()").get()
        if terrace and "yes" in terrace.lower():
            item_loader.add_value("terrace", True)

        lat="".join(response.xpath("//script[contains(.,'prop_lat')]/text()").get())
        if lat:
            latitude = lat.split("prop_lat =")[1].split(";")[0].strip()
            item_loader.add_value("latitude", latitude)
     
        lng= "".join(response.xpath("//script[contains(.,'prop_lng')]/text()").get())
        if lng:
            longitude = lng.split("prop_lng =")[1].split(";")[0].strip()
            item_loader.add_value("longitude", longitude)
        

        from datetime import datetime
        import dateparser
        available_date = response.xpath("//span[.='Available Date']/following-sibling::span/text()").get()
        if available_date:
            available_date=available_date.strip()
            if not "now" in available_date.lower():
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
        item_loader.add_value("landlord_name", "Tyne2Let")
        item_loader.add_value("landlord_phone", "0191 2286962")
        item_loader.add_value("landlord_email", "lettings@tyne2let.co.uk")

        
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("maison" in p_type_string.lower() or "house" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None

