# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import scrapy
import re 
import dateparser

class MySpider(Spider):
    name = 'richardmatthews_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.richardmatthews.com.au/lease/properties-for-lease/?keywords=&property_type%5B%5D=Apartment&property_type%5B%5D=Unit&price_min=&price_max=&bedrooms=&bathrooms=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.richardmatthews.com.au/lease/properties-for-lease/?keywords=&property_type%5B%5D=House&property_type%5B%5D=Semi+Detached&property_type%5B%5D=Townhouse&property_type%5B%5D=Villa&price_min=&price_max=&bedrooms=&bathrooms=",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://www.richardmatthews.com.au/lease/properties-for-lease/?keywords=&property_type%5B%5D=Studio&price_min=&price_max=&bedrooms=&bathrooms=",
                ],
                "property_type" : "studio",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        for item in response.xpath("//div[@class='image notsold']"):
            follow_url = response.urljoin(item.xpath(".//a/@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta['property_type']})
        
        next_button = response.xpath("//a[@class='next_page_link']/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse)
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        parking = response.xpath("//h1/span[@class='address']/text()[contains(.,'Parking')]").extract_first()
        if parking:
            return
        rented = response.xpath("//div[@class='opentimes']/div/p[@class='price'][.='LEASED']/text()").extract_first()
        if rented:
            return
        item_loader.add_value("external_source", "Richardmatthews_Com_PySpider_australia")
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/")[-2])
        item_loader.add_xpath("title", "//title/text()")
        
        price=""
        rent = "".join(response.xpath("//div[@class='opentimes']/div/p[@class='price'][contains(.,'$')]/text()").extract())     
        if rent:      
            if "-" in rent:
                price = rent.split("-")[0].replace("/"," ").split("$")[1].strip().split(" ")[0].replace(",","").strip()
            else:
                price =  rent.replace("/"," ").split("$")[1].split(" ")[0].replace(",","").strip()
            item_loader.add_value("rent",int(float(price))*4)
        item_loader.add_value("currency","USD")

        deposit = "".join(response.xpath("//div[@class='opentimes']/div/p[@class='price'][contains(.,'Bond')]/text()").extract())
        if deposit:
            dep =  deposit.split(" ")[1].strip()
            item_loader.add_value("deposit",dep)

        available_date="".join(response.xpath("//div[@class='opentimes']/div/p[@class='date']/text()").getall())
        if available_date:
            date2 =  available_date.split(":")[1].strip().replace("Immediate","now")
            date_parsed = dateparser.parse(
                date2, date_formats=["%m-%d-%Y"]
            )
            date3 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date3)

        item_loader.add_xpath("room_count", "//li[@class='bedrooms']/span[contains(@class,'num')]/text()")
        
        bathroom_count = response.xpath("//div[@class='fdRooms']/span[contains(.,'bathroom')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split(" ")[0])
        else:
            item_loader.add_xpath("bathroom_count", "//li[@class='bathrooms']/span[contains(@class,'num')]/text()")

        address = " ".join(response.xpath("//h2[contains(@class,'address-suburb')]//text()").getall())
        if address:
            item_loader.add_value("address", re.sub("\s{2,}", " ", address))
            city = response.xpath("//h2[contains(@class,'address-suburb')]/span[@class='suburb']/text()").extract_first()
            item_loader.add_value("city",city.split(" ")[0].strip() )
            item_loader.add_value("zipcode", "NSW "+city.split(" ")[-1])

        item_loader.add_xpath("latitude","substring-before(substring-after(//script[@type='text/javascript']//text()[contains(.,'LatLng')],'LatLng('),',')")
        item_loader.add_xpath("longitude","substring-before(substring-after(substring-after(//script[@type='text/javascript']//text()[contains(.,'LatLng')],'LatLng('),','),')')")

        desc =  " ".join(response.xpath("//div[@class='copy']/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

        images = [ x for x in response.xpath("//div[@id='gallery']/div/img/@src").getall()]
        if images:
            item_loader.add_value("images", images) 

        parking = "".join(response.xpath("//li[@class='carspaces']/span[contains(@class,'num')]/text()").extract())      
        if parking:
            (item_loader.add_value("parking", True) if "0" not in parking else item_loader.add_value("parking", False))
           

        pets_allowed = "".join(response.xpath("//div[@class='copy']/text()[contains(.,'Pets')]").extract())
        if pets_allowed:
            if "yes" in pets_allowed.lower() :
                item_loader.add_value("pets_allowed", True)
            elif "no" in pets_allowed.lower():
                item_loader.add_value("pets_allowed", False)

        elevator = "".join(response.xpath("//div[@class='copy']/text()[contains(.,'Lift')]").extract())
        if elevator:
            item_loader.add_value("elevator", True)


        item_loader.add_xpath("landlord_name", "//div[@id='agent_0']//div[@class='agent_contact_info']/h4/text()")
        item_loader.add_xpath("landlord_phone", "//div[@id='agent_0']//div[@class='agent_contact_info']/p[@class='agent_phone']/span/text()[normalize-space()]")

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "home" in p_type_string.lower() or "detached" in p_type_string.lower()):
        return "house"
    else:
        return None