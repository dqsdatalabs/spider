# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import itemloaders
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from python_spiders.helper import ItemClear
import re

class MySpider(Spider):
    name = 'leachandlang_com'
    execution_type='testing' 
    country='poland'
    locale='pl'
    external_source = "Leachandlang_PySpider_poland"
    custom_settings = {'HTTPCACHE_ENABLED': False}
    headers={
        ":path": "/_property/ListInfinite?query=21162&page=0&lang=en-GB&sortCol=&sortDir=&_=1637935935633",
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "accept-encoding": "gzip, deflate, br",
        "accept-language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
        "cookie":"ASP.NET_SessionId=ghtjurjm1xs1eeapdiymx4np; _culture=en-GB; _ga=GA1.2.499204186.1637758555; _gid=GA1.2.1877723351.1637911562; RecentlyViewed=[57486,57471,55181,57496,56399,57095]",
        "referer": "https://www.leachandlang.com/properties/apartments/all",
        "upgrade-insecure-requests": "1",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36",
        "x-requested-with": "XMLHttpRequest"
    }

 
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.leachandlang.com/_property/ListInfinite?query=21162&page=0&lang=en-GB&sortCol=&sortDir=&_=1637935935633"
                ]
            },

        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,headers=self.headers,
                )
    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 1)
        pagealt = response.meta.get('pagealt', 5)
        seen = False
        for url in response.xpath("//td//a/@href").getall():
            yield Request(response.urljoin(url), callback=self.populate_item)
            seen = True
        if page == 1 or seen:
            url = f"https://www.leachandlang.com/_property/ListInfinite?query=21162&page={page}&lang=en-GB&sortCol=&sortDir=&_=163793593563{pagealt}"
            yield Request(url, callback=self.parse,headers=self.headers, meta={"page": page+1,"pagealt":pagealt+1})


    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        dontallow=response.url
        if dontallow and "sale" in dontallow:
            return 
        property_type=response.xpath("//td[.='Property type']/following-sibling::td/text()").get()
        if get_p_type_string(property_type):
            item_loader.add_value("property_type", get_p_type_string(property_type))

        external_id=response.xpath("//td[.='Property number']/following-sibling::td/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id)
        square_meters=response.xpath("//td[.='Size']/following-sibling::td/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m²")[0].strip())
        rent=response.xpath("//td[.='Rent']/following-sibling::td/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("PLN")[0].replace(" ","").strip())
        item_loader.add_value("currency","PLN")
        room_count=response.xpath("//td[.='Number of rooms']/following-sibling::td/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.strip())
        floor=response.xpath("//td[.='Floor']/following-sibling::td/text()").get()
        if floor:
            item_loader.add_value("floor",floor.strip())
        elevator=response.xpath("//td[.='Elevator']/following-sibling::td/text()").get()
        if elevator and "Yes" in elevator:
            item_loader.add_value("elevator",True)
        images=[x for x in response.xpath("//div[@class='ms-slide-bgcont']//img//@src").getall()]
        if images:
            item_loader.add_value("images",images)
        available_date=response.xpath("//td[.='Available from']/following-sibling::td/text()").get()
        if available_date:
            item_loader.add_value("available_date",available_date)
        latitude=response.xpath("//script[contains(.,'google.maps.LatLng')]/text()").get()
        if latitude:
            item_loader.add_value("latitude",latitude.split("google.maps.LatLng")[-1].split(",")[0].replace("(","").replace(")",""))
        longitude=response.xpath("//script[contains(.,'google.maps.LatLng')]/text()").get()
        if longitude:
            item_loader.add_value("longitude",longitude.split("google.maps.LatLng")[-1].split(";")[0].split(",")[-1].replace("(","").replace(")",""))
        furnished=response.xpath("//td[.='Furniture']/following-sibling::td/text()").get()
        if furnished and "Yes" in furnished:
            item_loader.add_value("furnished",True)

        title = response.xpath("//div/h1/text()[.!='']").get()
        if title:
            item_loader.add_value("title",title)
            item_loader.add_value("address",title)
            city = title.split(",")[0]
            if city:
                item_loader.add_value("city",city)

        else:
            title = response.xpath("//div/h2/text()[. !='']").get()
            item_loader.add_value("title",title)
            item_loader.add_value("address",title)
            city = title.split(",")[0]
            if city:
                item_loader.add_value("city",city)

        item_loader.add_value("landlord_email","info@leachandlang.com")
        item_loader.add_value("landlord_phone","12 430 04 04")
        item_loader.add_value("landlord_name","Leach & Lang Property Consultants")

        images_src = response.xpath("//div[@class='ms-slide']/img/@data-src").getall()
        if images_src:
            images = ["https://www.leachandlang.com/" + img for img in images_src]
            item_loader.add_value("images",images)
        


     

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "huis" in p_type_string.lower() or "home" in p_type_string.lower() or "cottage" in p_type_string.lower()):
        return "house"
    else:
        return None