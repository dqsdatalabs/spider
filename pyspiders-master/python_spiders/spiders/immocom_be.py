# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy.linkextractors import LinkExtractor
from scrapy import Request 
from scrapy.selector import Selector
from python_spiders.items import ListingItem
from w3lib.html import remove_tags 
from python_spiders.loaders import ListingLoader
from scrapy import Request,FormRequest


class MySpider(Spider):
    name = "immocom_be" 
    execution_type = 'testing'
    country = 'belgium'
    locale='nl'
    external_source='immocom_PySpider_belgium'

    def start_requests(self):
        start_urls = [
            {
                "url" : "http://immocom.be/nl/residentieel-vastgoed/een-woning-huren/appartement",
                "property_type" : "apartment",
                "type":"2",
                "property_type1":"2"
            },
            {
                "url" : "http://immocom.be/nl/residentieel-vastgoed/een-woning-huren/huis",
                "property_type" : "house",
                "type":"1",
                "property_type1":"1"
            },
        ] # LEVEL 1

        for url in start_urls:
            yield Request(url=url.get('url'),callback=self.parse, meta={'property_type': url.get('property_type'),'type': url.get('type'),'property_type1': url.get('property_type1')})
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//a[@class='card bien']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            seen = True
        if page == 2 or seen:
            type = str(response.meta.get("type"))
            property_type1 = str(response.meta.get("property_type1"))

            formdata={
                "limit1": "12",
                "limit2": "12",
                "serie": "1",
                "filtre": "filtre_cp",
                "market": "",
                "lang": "nl",
                "type": type,
                "goal": "1",
                "property-type":property_type1,
                "goal": "1",
                "search": "1",
                "property-type":property_type1,
                "goal": "1",
                "search": "1",
            }
            
            next_page = "http://immocom.be/Connections/request/xhr/infinite_projects.php"
            if next_page:        
                yield FormRequest(next_page,callback=self.parse,formdata=formdata,meta={'property_type': response.meta.get('property_type')})
    
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source",self.external_source)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type",response.meta.get('property_type'))

        rent=response.xpath("//b[.='Huur / maand']/parent::h3/parent::td/following-sibling::td/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[0])
        item_loader.add_value("currency","EUR")
        adres=response.xpath("//b[.='Adres']/parent::h3/parent::td/following-sibling::td/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        if not adres:
            adres=response.xpath("//b[.='Postcode']/parent::h3/parent::td/following-sibling::td/text()").get()
            if adres:
                item_loader.add_value("address",adres)
        zipcode=response.xpath("//b[.='Postcode']/parent::h3/parent::td/following-sibling::td/text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.split("-")[0].strip())
            city=zipcode.split("-")[-1].strip()
            item_loader.add_value("city",city)
        energy_label=response.xpath("//b[.='Energie score']/parent::h3/parent::td/following-sibling::td/text()").get()
        if energy_label:
            energy = energy_label.replace("\r","").replace("\n","").split("k")[0]
            if not "Op" in energy:
                item_loader.add_value("energy_label",energy_label_calculate(int(float(energy.replace(",",".")))))
        square_meters=response.xpath("//b[.='Vloeroppervlakte']/parent::h3/parent::td/following-sibling::td/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m²")[0])
        external_id=response.xpath("//text()[contains(.,'Referentie')]/following-sibling::b/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id)
        latitude=response.xpath("//script[contains(.,'my_lat')]/text()").get()
        if latitude:
            item_loader.add_value("latitude",latitude.split("meters")[0].split("my_lat")[1].split(";")[0].replace("=","").strip())
        longitude=response.xpath("//script[contains(.,'my_lat')]/text()").get()
        if longitude:
            item_loader.add_value("longitude",longitude.split("meters")[0].split("my_long")[1].split(";")[0].replace("=","").strip())
        room_count=response.xpath("//b[.='Aantal kamers	']/parent::h3/parent::td/following-sibling::td/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        parking=response.xpath("//b[.='Garage(s)']/parent::h3/parent::td/following-sibling::td/text()").get()
        if parking and parking=="1":
            item_loader.add_value("parking",True)
        images=[x for x in response.xpath("//a[@data-fancybox='gallery']/@href").getall()]
        if images:
            item_loader.add_value("images",images)
        item_loader.add_value("landlord_name","TREVI")
        phone=response.xpath("//span/a[contains(@href,'tel')]/text()").get()
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