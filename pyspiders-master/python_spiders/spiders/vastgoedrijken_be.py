# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request, FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader 
import json 
from  geopy.geocoders import Nominatim  
from html.parser import HTMLParser
import math 
import re
from scrapy.selector import Selector

class MySpider(Spider):
    name = 'vastgoedrijken_be' 
    execution_type='testing'
    country='belgium'
    locale='nl' # LEVEL 1
    external_source="Vastgoedrijken_PySpider_belgium"
    formdata={
        "spage": "1",
        "sbuy": "0"
    }
    
    def start_requests(self):
        start_urls = [
            {
                "url" : "https://www.vastgoedrijken.be/ajax/search/get_immo.php",
            },

        ] # LEVEL 1

        for url in start_urls:
            yield FormRequest(url=url.get('url'),callback=self.parse,formdata=self.formdata)
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        data=json.loads(response.body)['properties']
        list=Selector(text=data).xpath("//a[@class='property mb-4 mb-md-5']/@href").getall()
        for item in list:
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            seen = True
        if page == 2 or seen:
            formdata={
                "scrollTop": "true",
                "spage": str(page),
                "sbuy": "0"
            }
            next_page = f"https://www.vastgoedrijken.be/ajax/search/get_immo.php"
            if next_page:        
                yield FormRequest(next_page,callback=self.parse,formdata=formdata,meta={'property_type': response.meta.get('property_type'),'page':page+1})


    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link",response.url)
        
        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)
        adres="".join(response.xpath("//div[@class='line-height-lg']//text()").getall())
        if adres:
            adres=re.sub('\s{2,}', ' ', adres.strip())
            item_loader.add_value("address",adres)
        zipcode=response.xpath("//div[@class='page-header-content']/h1/span/text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.replace("|","").strip())
        rent=response.xpath("//div[@class='price']/text()").get()
        if rent:
            rent=rent.split("€")[-1].strip()
            if not "verhuurd" in rent:
                item_loader.add_value("rent",rent)
        item_loader.add_value("currency","EUR")
        description="".join(response.xpath("//div[@class='collapse-content-container']//p//text()").getall())
        if description:
            item_loader.add_value("description",description)
        images=[x for x in response.xpath("//img//@src").getall()]
        if images:
            item_loader.add_value("images",images)
        floor=response.xpath("//td[.='Aantal bouwlagen']/following-sibling::td/text()").get()
        if floor:
            item_loader.add_value("floor",floor)
        property_type=response.xpath("//li[@class='breadcrumb-item active']/text()").get()
        if property_type and "Appartement" in property_type:
            item_loader.add_value("property_type","apartment")
        if property_type and "Huis" in property_type:
            item_loader.add_value("property_type","house")
        if property_type and "Handelspand" in property_type:
            return 
        room_count=response.xpath("//td[.='Slaapkamers']/following-sibling::td/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        bathroom_count=response.xpath("//td[.='Badkamer']/following-sibling::td/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.strip().split(" ")[0])
        terrace=response.xpath("//td[.='Terras']/following-sibling::td/text()").get()
        if terrace and "aanwezig" in terrace:
            item_loader.add_value("terrace",True)
        energy_label=response.xpath("//strong[.='EPC:']/following-sibling::text()").get()
        if energy_label:
            energy = energy_label.replace("\r","").replace("\n","").split("k")[0]
            item_loader.add_value("energy_label",energy_label_calculate(int(float(energy.replace(",",".")))))
        washing_machine=response.xpath("//td[.='Wasplaats']/following-sibling::td/text()").get()
        if washing_machine and washing_machine=='Aanwezig':
            item_loader.add_value("washing_machine",True)
        phone=response.xpath("//a[.='Contact']/following-sibling::a/text()").get()
        if phone:
            item_loader.add_value("landlord_phone",phone)
        yield item_loader.load_item()

def energy_label_calculate(energy_number):
    energy_number = int(energy_number)
    energy_label = ""
    if energy_number <= 50:
        energy_label = "A"
    elif energy_number > 50 and energy_number <= 90:
        energy_label = "B"
    elif energy_number > 90 and energy_number <= 150:
        energy_label = "C"
    elif energy_number > 150 and energy_number <= 230:
        energy_label = "D"
    elif energy_number > 230 and energy_number <= 330:
        energy_label = "E"
    elif energy_number > 330 and energy_number <= 450:
        energy_label = "F"
    elif energy_number > 450:
        energy_label = "G"
    return energy_label