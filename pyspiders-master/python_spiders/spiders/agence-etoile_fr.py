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
import dateparser
class MySpider(Spider):
    name = 'agence-etoile_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="AgenceEtoile_PySpider_france"
    custom_settings = {"HTTPCACHE_ENABLED": False}
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.agence-etoile.fr/en/property/?status=For%20Rent&city%5B0%5D&price-min=60&price-max=50330&sqft-min=0&sqft-max=1120#038;city%5B0%5D&price-min=60&price-max=50330&sqft-min=0&sqft-max=1120",
                ],
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse)
    # 1. FOLLOWING
    def parse(self, response):
        border=response.xpath("//span[@class='page-numbers dots']/following-sibling::a[@class='page-numbers']/text()").get()
        page = response.meta.get('page', 2)
        seen = False
        for item in response.xpath("//div[@class='property_list_single_left']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        if page or seen:
            if border:
                if page<=int(border)+3:
                    next_page = f"https://www.agence-etoile.fr/en/property/page/{page}/?status=For%20Rent&city%5B0%5D&price-min=60&price-max=50330&sqft-min=0&sqft-max=1120#038;city%5B0%5D&price-min=60&price-max=50330&sqft-min=0&sqft-max=1120"
                    if next_page:
                        yield Request(response.urljoin(next_page), callback=self.parse,meta={"page": page+1,})
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source",self.external_source)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        title=response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title",title)
        dontallow=title
        if dontallow and ("garage" in dontallow.lower() or "commercial" in dontallow.lower() or "offices" in dontallow.lower() ):
            return 
        f_text =response.xpath("//title/text()").get()
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))

        rent=response.xpath("//div[@class='single_property_header']/div//p[contains(.,'month')]/text()").get()
        if rent:
            item_loader.add_value("rent",rent.replace(" ",""))
        item_loader.add_value("currency","EUR")
        adres=response.xpath("//div[@class='single_property_header']/div[2]//p[contains(.,'month')]/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        description="".join(response.xpath("//div[@class='single_propert_desc']/text()").getall())
        if description:
            item_loader.add_value("description",description)
        deposit=response.xpath("//div[@class='single_propert_desc']/text()").getall()
        if deposit:
            for i in deposit:
                if "deposit" in i:
                    item_loader.add_value("deposit",i.split(":")[-1].strip())
        square_meters=response.xpath("//sup[.='2']/preceding-sibling::text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m")[0].split(".")[0].strip())
        room_count=response.xpath("//img[contains(@src,'pieces')]/following-sibling::p/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        bathroom_count=response.xpath("//p[contains(.,'Number of bathrooms')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.split(":")[-1].strip())
        energy_label=response.xpath("//img[contains(@src,'picto_dpe')]/following-sibling::p/text()").get()
        if energy_label:
            item_loader.add_value("energy_label",energy_label)
        terrace=response.xpath("//p[contains(.,'Terrace')]/text()").get()
        if terrace and "Yes" in terrace:
            item_loader.add_value("terrace",True)
        furnished=response.xpath("//img[contains(@src,'picto_meuble')]/following-sibling::p/text()").get()
        if furnished and "Non" in furnished:
            item_loader.add_value("furnished",True)
        
        images=[x for x in response.xpath("//img[contains(@src,'uploads')]/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        item_loader.add_value("landlord_name","Agence Etoile")
        item_loader.add_value("landlord_email","aix@agence-etoile.com")
        item_loader.add_value("landlord_phone","+33 4 42 91 32 32")



        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "villa" in p_type_string.lower() or "home" in p_type_string.lower() or "cottage" in p_type_string.lower()):
        return "house"
    else:
        return None