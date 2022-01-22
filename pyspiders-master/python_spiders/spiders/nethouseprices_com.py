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
    name = 'nethouseprices_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    thousand_separator = ','
    scale_separator = '.'      
    custom_settings = {              
        # "PROXY_ON" : True, 
        "CONCURRENT_REQUESTS": 3,        
        "COOKIES_ENABLED": False, 
        "HTTPCACHE_ENABLED": False,       
        "RETRY_TIMES": 3,  
        # "FEED_FORMAT": 'json', 
        # "FEED_URI": 'stdout:'     
    }
    download_timeout = 120
    def start_requests(self):
        
        yield Request(
            url="https://nethouseprices.com/properties-to-rent/",
            callback=self.jump,
        )
    
    def jump(self,response):
        for item in response.xpath("//table[contains(@class,'listing-table')]//tr/td[1]"):
            url = response.urljoin(item.xpath(".//@href").get())
            yield Request(url, callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//a[contains(.,'More Detail')]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
        
        next_page = response.xpath("//a[contains(.,'Next')]/@href").get()
        if next_page:
            url = response.urljoin(next_page)
            yield Request(url, callback=self.parse)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        status=response.xpath("//div[@class='property-header-address']//p//text()").get()
        if status and "parking" not in status.lower():
            item_loader.add_value("external_link", response.url)
            item_loader.add_value("property_type", response.meta.get('property_type'))
            item_loader.add_value("external_source", "Nethouseprices_PySpider_united_kingdom")
            item_loader.add_value("external_id", response.url.split("details/")[-1].split("/")[0])
            dontallow=response.xpath("//span[@class='address-header ']/text()").get()
            if dontallow and "commercial" in dontallow.lower():
                return 
            item_loader.add_xpath("title", "//div[contains(@class,'property-header-address')]//span[contains(@class,'address-header')]//text()")
            if "nethouseprices.com/" not in response.url:
                # print(response.url)
                return
            rent = response.xpath("//div[contains(@class,'property-header-address')]//span[contains(@class,'address-header')]//text()").get()
            if rent:
                rent = rent.split(" - ")[-1]
                if "week" in rent.lower():
                    rent = rent.lower().split('£')[-1].split('week')[0].strip().replace(',', '').replace('\xa0', '')
                    item_loader.add_value("rent", str(int(float(rent)*4)))    
                    item_loader.add_value("currency", "GBP")    
                else:
                    item_loader.add_value("rent_string",rent )  
            address = response.xpath("//div[contains(@class,'property-header-address')]//p[last()]/text()").get()
            if address:
                item_loader.add_value("address", address.strip())
                # city_zipcode = address.split(",")[-1].strip()
                # if city_zipcode.isalpha():
                #     return
                # elif "," in city_zipcode:
                #     city_zipcode=city_zipcode.split(",")[1]
                # else:
                #     city = response.xpath("//div[@class='property-header-address']//p[2]//text()").get()
                #     if city:
                #         item_loader.add_value("city", city)
                #     else:
                #         item_loader.add_value("city", "Birmingham")

            city = "".join(response.xpath("//ul[contains(@class,'orange-circles')]//li//a[contains(@title,'Property To Rent')]//@href").get())
            if city:
                city = city.split("/")[-3:-2]
                item_loader.add_value("city", city)

            zipcode = response.xpath("(//div[contains(@class,'push-right padding-left')])[1]//p[contains(.,' ')][3]//text()").get()
            if zipcode:
                item_loader.add_value("zipcode", zipcode)

            desc = " ".join(response.xpath("//div[@class='property-main-description']/div//text()").getall())
            if desc:
                item_loader.add_value("description", desc.strip())
            property_type=response.xpath("//span[@class='address-header ']/text() | //span[@class='address-header price-changed']/text()").get()
            if property_type:
                if "flat" in property_type.lower():
                    item_loader.add_value("property_type","apartment")
                elif "house" in property_type.lower():
                    item_loader.add_value("property_type","house")
                else:
                    item_loader.add_value("property_type","house")



            room_count = response.xpath("//div[contains(@class,'property-header-address')]//span[contains(@class,'address-header')]//text()[contains(.,'Bedroom')]").get()
            if room_count:
                item_loader.add_value("room_count", room_count.split("Bedroom")[0].strip().split(" ")[-1])
            terrace = response.xpath("//div[@class='property_features']//li/text()[contains(.,'Terrace') or contains(.,'terrace')]").get()
            if terrace:
                item_loader.add_value("terrace", True)
            parking = response.xpath("//div[@class='property_features']//li/text()[contains(.,'Parking') or contains(.,'parking')]").get()
            if parking:
                item_loader.add_value("parking", True)
            furnished = response.xpath("//div[@class='property_features']//li/text()[contains(.,'Furnished') or contains(.,'furnished')]").get()
            if furnished:
                if "unfurnished" in furnished.lower():
                    item_loader.add_value("furnished", False)
                elif "furnished" in furnished.lower():
                    item_loader.add_value("furnished", True)
                    
            images = [x for x in response.xpath("//div[@id='slides-container']/div/img/@src").getall()]
            if images:
                item_loader.add_value("images", images)
            lat_lng = response.xpath("//script[contains(.,'var lat =')]//text()").get()
            if lat_lng:
                item_loader.add_value("latitude", lat_lng.split("var lat = '")[-1].split("'")[0].strip())
                item_loader.add_value("longitude", lat_lng.split("var lng = '")[-1].split("'")[0].strip())
            landlord_phone = response.xpath("//div[contains(@class,'agent-contact-details')]//span[@class='revealNumber']/text()").get()
            if landlord_phone:
                item_loader.add_value("landlord_phone", landlord_phone.strip())
            landlord_email = response.xpath("//div[contains(@class,'agent-contact-details')]//p[span[.='Email: ']]/text()").get()
            if landlord_email:
                item_loader.add_value("landlord_email", landlord_email.strip())
            else:
                landlord_email = response.xpath("(//span[contains(.,'Email')]//following-sibling::text())[2]").get()
                if landlord_email:
                    item_loader.add_value("landlord_email", landlord_email)

            item_loader.add_xpath("landlord_name", "//div[@class='property-main-descriptio hide-mobile']//p/strong/text()")
            
            yield item_loader.load_item()         