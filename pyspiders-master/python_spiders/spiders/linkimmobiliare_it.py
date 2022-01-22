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
class MySpider(Spider):
    name = 'linkimmobiliare_it'
    execution_type='testing'
    country='italy'
    locale='it' 
    external_source = "Linkimmobiliare_PySpider_italy"

    def start_requests(self):
        start_urls = [ 
            {
                "url": [
                    "https://www.linkimmobiliare.it/properties-search/?status=affitto&type=appartamento-residenziale",
                ],
                "property_type": "apartment",
            },
	        {
                "url": [
                    "https://www.linkimmobiliare.it/properties-search/?status=affitto&type=casa-indipendente",
                    "https://www.linkimmobiliare.it/properties-search/?status=affitto&type=attico",
                    "https://www.linkimmobiliare.it/properties-search/?status=affitto&type=casale",
                    "https://www.linkimmobiliare.it/properties-search/?status=affitto&type=villa"
                ],
                "property_type": "house",
                
            },
            {
                "url": [
                    "https://www.linkimmobiliare.it/properties-search/?status=affitto&type=stanza",
                ],
                "property_type": "room",
                
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):        
        for item in response.xpath("//a[contains(.,'Vedi Immobile')]"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
        
        next_page = response.xpath("//a[contains(@class,'next')]/@href").get()
        if next_page:
            yield Request(next_page, callback=self.parse, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        external_id=response.xpath("//div[@class='rh_property__id']//p[2]/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.replace("\u00a0",""))

        title=response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title",title)
        adres=response.xpath("//p[@class='rh_page__property_address']/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        city=response.xpath("//p[@class='rh_page__property_address']/text()").get()
        if city:
            city=adres.split(",")[-1].replace("\t","")
            item_loader.add_value("city",city)
        zipcode=response.xpath("//p[@class='rh_page__property_address']/text()").get()
        if zipcode:

            zipcode=re.findall("\d+",zipcode)
            item_loader.add_value("zipcode",zipcode)
        rent=response.xpath("//p[@class='price']/text()").get()
        if rent:
            rent=rent.split("€")[-1]
            item_loader.add_value("rent",rent)
            item_loader.add_value("currency","EUR")
        description=" ".join(response.xpath("//div[@class='rh_content']/p//text() | //div[@class='col-xs-12 description-text text-expanded']/div/text()").getall())
        if description:
            item_loader.add_value("description",description)
        room_count=response.xpath("//h4[.='Locali']/following-sibling::div/span/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        bathroom_count=response.xpath("//h4[.='Bagni']/following-sibling::div/span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        square_meters=response.xpath("//h4[.='Superficie']/following-sibling::div/span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters)
        energy_label=response.xpath("//span[.='Classe energetica:']/following-sibling::span/text()").get()
        if energy_label:
            item_loader.add_value("energy_label",energy_label)
        parking=response.xpath("//li[@class='rh_property__feature']//a//text()[.='posto auto']").get()
        if parking:
            item_loader.add_value("parking",True)
        images=[x for x in response.xpath("//a[@rel='gallery_real_homes']/img/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        floor=response.xpath("//span[.='Numero piano:']/following-sibling::span/text()").get()
        if floor:
            floor=re.findall("\d+",floor)
            item_loader.add_value("floor",floor)
        item_loader.add_value("landlord_name","LINK IMMOBILIARE")
        item_loader.add_value("landlord_phone","+39 06 320 4966")
        item_loader.add_value("landlord_email","info@linkimmobiliare.it")

        yield item_loader.load_item()