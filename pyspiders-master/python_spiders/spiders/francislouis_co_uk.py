# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser 
import re

class MySpider(Spider):
    name = 'francislouis_co_uk' 
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source='Francislouis_Co_PySpider_united_kingdom'
    def start_requests(self):
        start_urls = [
            { "url": "https://www.francislouis.co.uk/properties-to-let"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse)

    def parse(self, response):
        property_type = response.meta.get("property_type")
 
        for item in response.xpath("//div[@id='smallProps']//div[@class='eapow-property-thumb-holder']//a//@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":property_type})

        pagination = response.xpath("//div[contains(@class,'span12')]//ul[@class='pagination-list']//li//a[contains(.,'Next')]//@href").get()
        if pagination:
            yield Request(response.urljoin(pagination), callback=self.parse, meta={"property_type":property_type})

    def populate_item(self, response):

        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source",self.external_source)
        property_type = " ".join(response.xpath("//div[contains(@class,'desc')]//p//text()").getall()).strip()
        if get_p_type_string(property_type): item_loader.add_value("property_type", get_p_type_string(property_type))

        property_check=item_loader.get_output_value("property_type")
        if not property_check:
            item_loader.add_value("property_type","apartment")


        title = " ".join(response.xpath("//h1//text()").extract())
        item_loader.add_value("title", title)
        address = " ".join(response.xpath("//div[contains(@class,'eapow-mainaddress')]/address//text()").extract())
        if address:
            item_loader.add_value("address", address)
        

        ext_id = response.xpath("//div[@class='eapow-sidecol'][b[contains(.,'Ref ')]]/text()").extract_first()     
        if ext_id:   
            item_loader.add_value("external_id",ext_id.replace(":","").strip())

        city_zipcode = response.xpath("//div[contains(@class,'eapow-mainaddress')]/address/text()").extract_first()     
        if city_zipcode:   
            zipcode =" ".join(city_zipcode.split(" ")[1:])
            city = city_zipcode.split(" ")[0].strip()  
            item_loader.add_value("city",city.strip())
            item_loader.add_value("zipcode",zipcode.strip())
 
        rent = response.xpath("//small[@class='eapow-detail-price']//text()").get()
        if rent:
            if "week" in rent.lower():
                price = rent.strip().split("£")[1].replace(",","").replace(".","")
                price = int(price)*4
                item_loader.add_value("rent", price)
            else:
                if "monthly" in rent.lower():
                    price = rent.strip().split("£")[1].split(".")[0].replace(",","").replace(".","")
                    item_loader.add_value("rent", price)
                price = rent.strip().split("£")[1].replace(",","").replace(".","")
                item_loader.add_value("rent", price)
        item_loader.add_value("currency", "GBP")
            
        desc = " ".join(response.xpath("//div[contains(@class,'pow-desc-wrapper')]/p//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
            if "Available from" in desc: 
                try:
                    available_date = desc.split("Available from")[1].split(".")[0].strip()
                    date_parsed = dateparser.parse(available_date, languages=['en'])
                    if date_parsed:
                        date2 = date_parsed.strftime("%Y-%m-%d")
                        item_loader.add_value("available_date", date2)          
                except:
                    pass

        room_count = response.xpath("//div[@class='span12']//i[@class='flaticon-sofa']//following-sibling::span//text()").get()
        if room_count and room_count.strip() !="0":
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//div[@class='span12']//i[@class='flaticon-bath']//following-sibling::span//text()").get()
        if bathroom_count and bathroom_count.strip() !="0":
            item_loader.add_value("bathroom_count", bathroom_count)

        images = [x for x in response.xpath("//div[@id='eapowgalleryplug']//a//@href").getall()]
        if images:
            item_loader.add_value("images", images)   

        floor_plan_images = [x for x in response.xpath("//div[@id='eapowfloorplanplug']//a/@href").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images) 

        balcony = response.xpath("//ul[@id='starItem']/li//text()[contains(.,'Balcony') or contains(.,'balcony')]").get()
        if balcony:
            item_loader.add_value("balcony", True) 
                
        terrace = response.xpath("//ul[@id='starItem']/li//text()[contains(.,'Terrace') or contains(.,'terrace') ]").get()
        if terrace:
            item_loader.add_value("terrace", True) 

        parking = response.xpath("//ul[@id='starItem']/li//text()[contains(.,'Parking') or contains(.,'parking')]").get()
        if parking:
            item_loader.add_value("parking", True) 


        latitude_longitude = response.xpath(
            "//script[contains(@type,'text/javascript')]//text()[contains(.,'lat')]").get()
        if latitude_longitude:
            latitude = latitude_longitude.split(
                'lat: "')[1].split('",')[0]
            longitude = latitude_longitude.split(
                'lon: "')[1].split('",')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_phone", "01392 243077")
        item_loader.add_value("landlord_email", "info@francislouis.co.uk")
        item_loader.add_value("landlord_name", "FrancisLouisResidential")

        yield item_loader.load_item()
        
def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("studio" in p_type_string.lower() or "t1" in p_type_string.lower()):
        return "studio"
    elif p_type_string and ("appartement" in p_type_string.lower() or "f1" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "t2" in p_type_string.lower() or "t3" in p_type_string.lower() or "t4" in p_type_string.lower() or "t5" in p_type_string.lower() or "apartment" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("maison" in p_type_string.lower() or "villa" in p_type_string.lower() or "house" in p_type_string.lower() or "home" in p_type_string.lower()):
        return "house"
    elif p_type_string and "chambre" in p_type_string.lower():
        return "room"   
    else:
        return None