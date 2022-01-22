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
    name = 'realtycare_be' 
    execution_type='testing'
    country='belgium'
    locale='fr' # LEVEL 1
    external_source="Realtycare_PySpider_belgium"
    
    def start_requests(self):
        start_urls = [
            {
                "url" : ["https://www.realtycare.be/fr/biens/?_type_of_transaction=a-louer&_property_type=appartement"],
                "property_type": "apartment"
            },

        ] # LEVEL 1

        for url in start_urls:
            for item in url.get('url'):
                yield Request(url=item,callback=self.parse,meta={"property_type":url.get('property_type')})
    def parse(self, response):
        for item in response.xpath("//a[@class='wpgb-card-layer-link']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link",response.url)
        item_loader.add_value("property_type",response.meta.get("property_type"))

        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)
        adres="".join(response.xpath("//div[@class='elementor-text-editor elementor-clearfix']/text()").getall())
        if adres:
            item_loader.add_value("address",adres.replace("\t","").replace("\n",""))
        rent=response.xpath("//span[@class='price']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[0].strip())
        item_loader.add_value("currency","EUR")
        external_id=response.xpath("//span[@class='Référence']/following-sibling::span/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id)
        room_count=response.xpath("//span[.='Chambres']/following-sibling::span/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        bathroom_count=response.xpath("//span[.='Salle de bain']/following-sibling::span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        square_meters=response.xpath("//span[.='Superficie totale']/following-sibling::span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m")[0].strip())
        description="".join(response.xpath("//div[@class='elementor-shortcode']/text()").getall())
        if description:
            item_loader.add_value("description",description.replace("\t","").replace("\n",""))
        adres=response.xpath("//span[.='Ville']/following-sibling::span/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        images=[x for x in response.xpath("//figure//img//@src").getall()]
        if images:
            item_loader.add_value("images",images)
        energy_label=response.xpath("//span[.='Classe energétique']/following-sibling::span/img/@src").get()
        if energy_label:
            item_loader.add_value("energy_label",energy_label.split("peb_")[-1].split(".svg")[0])
        item_loader.add_value("landlord_name","Realty Care Property Management")
        item_loader.add_value("landlord_email","info@realtycare.be")
        item_loader.add_value("landlord_phone","+32 2 450 56 56")
        yield item_loader.load_item()