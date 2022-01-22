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
    name = 'directcasa_it' 
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "Directcasa_PySpider_italy"
    start_urls = ["https://www.directcasa.it/status/in-affitto/"]


    headers={
        "accept": "application/json, text/javascript, */*; q=0.01",
        "accept-encoding": "gzip, deflate, br",
        "accept-language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
        "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
        "cookie": "_ga=GA1.2.672060784.1631511560; PHPSESSID=b578d99d2651ca8c3549ea80c6829488; _gid=GA1.2.2132398707.1634124705; _gat_gtag_UA_135886907_1=1",
        "referer": "https://www.directcasa.it/status/in-affitto/page/2",
        "sec-fetch-site": "same-origin",
        "sec-fetch-user": "?1",
        "upgrade-insecure-requests": "1",
        "user-agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.71 Mobile Safari/537.36",
        "x-requested-with": "XMLHttpRequest",
    }

    # 1. FOLLOWING
    def parse(self, response):
        page=response.meta.get("page",2)
        seen=False
        
        for item in response.xpath("//a[@class='hover-effect']/@href").extract():
            follow_url = response.urljoin(item)
            yield FormRequest(follow_url, callback=self.populate_item)
            seen=True
        if page==2 or seen:
          
            next_page = f"https://www.directcasa.it/status/in-affitto/page/{page}/"
            if next_page:
                yield FormRequest(next_page, callback=self.parse,headers=self.headers,meta={"page":page+1})   

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        status = response.xpath("//li[strong[contains(.,'Contratto')]]/text()").get()
        if "vendita" in status.lower():
            return
        item_loader.add_value("external_link", response.url)
        property_type = response.xpath("//h1/text()").get()
        if get_p_type_string(property_type):
            item_loader.add_value("property_type", get_p_type_string(property_type))
        else:
            if get_p_type_string(response.xpath("//li[strong[contains(.,'Tipo')]]/text()").get()):
                item_loader.add_value("property_type", get_p_type_string(response.xpath("//li[strong[contains(.,'Tipo')]]/text()").get()))
            else: 
                return
        item_loader.add_value("external_source", self.external_source)
    
        external_id ="".join( response.xpath(
            "//link[@rel='shortlink']//@href").get())
        if external_id:
            external_id="".join(external_id.split("=")[1])
            item_loader.add_value("external_id", external_id)

        title = response.xpath(
            "//title//text()").get()
        if title:
            item_loader.add_value("title", title)

        room_count = response.xpath(
            "(//ul[@class='list-three-col']//li[contains(.,'Vani')]//following-sibling::text())[1]").get()
        if room_count:
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath(
            "(//ul[@class='list-three-col']//li[contains(.,'Bagni')]//following-sibling::text())[1]").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        city = response.xpath(
            "//div[@id='address']//ul//li[@class='detail-city']//strong//following-sibling::text()").get()
        if city:
            item_loader.add_value("city", city)

        address = response.xpath(
            "//div[@id='address']//ul//li[@class='detail-state']//strong//following-sibling::text()").get()
        if address:
            item_loader.add_value("address", address+" "+city)

        description = response.xpath(
            "//div[@id='description']//p//text()").getall()
        if description:
            item_loader.add_value("description", description)

        else:
            description = response.xpath(
                "//div[@id='description']//div//text()").getall()
            if description:
                item_loader.add_value("description", description)

        rent = response.xpath(
            "//div[@class='alert alert-info']//ul//li//strong[contains(.,'Prezzo:')]//following-sibling::text()").get()
        if rent:
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")

        square_meters = response.xpath(
            "//div[@class='alert alert-info']//ul//li//strong[contains(.,'Superficie')]//following-sibling::text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters)

        parking = response.xpath(
            "(//ul[@class='list-three-col']//li[contains(.,'Box auto')]//text())[1]").get()
        if parking:
            item_loader.add_value("parking", True)
        else:
            item_loader.add_value("parking", False)

        images = [response.urljoin(x) for x in response.xpath(
            "//div[contains(@class,'media')]//a//img//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", "Direct Casa")
        item_loader.add_value("landlord_phone", "081 1966 8300")
        item_loader.add_value(
            "landlord_email", "info@directcasa.it")

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and ("appartament" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "loft" in p_type_string.lower() or "attico" in p_type_string.lower() or "mansarda" in p_type_string.lower()):
        return "house"
    else:
        return None