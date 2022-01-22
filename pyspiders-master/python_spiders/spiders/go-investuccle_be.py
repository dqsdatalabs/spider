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
from scrapy.selector import Selector

class MySpider(Spider):
    name = 'go-investuccle_be' 
    execution_type='testing'
    country='belgium'
    locale='nl' # LEVEL 1
    external_source="GoInvestuccle_PySpider_belgium"
    
    def start_requests(self):
        start_urls = [
            {
                "url" : ["http://www.go-investuccle.be/index.php?action=list&?ctypmandatmeta=l&action=list&ctypmandatl=1&ctypmandatlm=1&ctypmandatmeta=l&cbien=&ctypmeta=appt"],
                "property_type": "apartment"
            },
            {
                "url": [
                    "http://www.go-investuccle.be/index.php?action=list&?ctypmandatmeta=l&action=list&ctypmandatl=1&ctypmandatlm=1&ctypmandatmeta=l&cbien=&ctypmeta=mai",
                ],
                "property_type": "house"
            },

        ] # LEVEL 1

        for url in start_urls:
            for item in url.get('url'):
                yield Request(url=item,callback=self.parse,)
    def parse(self, response):
        for item in response.xpath("//div[@class='picture']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        next_page = response.xpath("//a[.='Suivant >']/@href").get()
        if next_page:        
            yield Request(response.urljoin(next_page),callback=self.parse,meta={'property_type': response.meta.get('property_type')})


    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link",response.url)

        title=response.xpath("//div[@class='tooltips']/following-sibling::h2/text()").get()
        if title:
            item_loader.add_value("title",title)
        adres=response.xpath("//div[@class='tooltips']/following-sibling::h2/span/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        property_type=response.xpath("//div[@class='tooltips']/following-sibling::h2/text()").get()
        if property_type and ("appartement" in property_type.lower() or "apprtement" in property_type.lower()):
            item_loader.add_value("property_type","apartment")
        if property_type and ("villa" in property_type.lower() or "maison" in property_type.lower() or "house" in property_type.lower()):
            item_loader.add_value("property_type","house")
        rent=response.xpath("//b[contains(.,'Prix')]/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split(":")[-1].split("€")[0].replace(" ","").replace(".",""))
        item_loader.add_value("currency","EUR")
        description=response.xpath("//div[@class='headline']/following-sibling::p/text()").getall()
        if description:
            item_loader.add_value("description",description)
        properttytype2=item_loader.get_output_value("property_type")
        if not properttytype2:
            property_type="".join(response.xpath("//div[@class='headline']/following-sibling::p/text()").getall())
            if property_type and ("appartement" in property_type.lower() or "apprtement" in property_type.lower()):
               item_loader.add_value("property_type","apartment")
            if property_type and ("villa" in property_type.lower() or "maison" in property_type.lower() or "house" in property_type.lower()):
               item_loader.add_value("property_type","house")
        external_id=response.xpath("//p[contains(.,'Réf.')]/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.split(":")[-1])
        room_count=response.xpath("//li[contains(.,'Chambres')]/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.strip().split(" ")[0])
        bathroom_count=response.xpath("//li[contains(.,'Salles de bains')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.strip().split(" ")[0])
        square_meters=response.xpath("//li[contains(.,'Surface habitable:')]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split(":")[-1].split("m²")[0].strip())
        terrace=response.xpath("//li[contains(.,'Terrasse')]/text()").get()
        if terrace:
            item_loader.add_value("terrace",True)
        energy_label=response.xpath("//strong[.='Consommation énergétique:']/parent::p/img/following-sibling::text()").get()
        if energy_label:
            energy = energy_label.replace("\r","").replace("\n","").split("k")[0]
            item_loader.add_value("energy_label",energy_label_calculate(int(float(energy.replace(",",".")))))
        images=[x for x in response.xpath("//img[contains(@src,'phpThumb')]/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        item_loader.add_value("landlord_name","Go Invest")
        item_loader.add_value("landlord_phone","+322 373 01 50")



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