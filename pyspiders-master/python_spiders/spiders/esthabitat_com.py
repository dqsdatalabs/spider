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
    name = 'esthabitat_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="Esthabitat_PySpider_france"
    custom_settings = {
        "HTTPCACHE_ENABLED": False,
    } 
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://esthabitat.com/locations/",
                ],
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse)
    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='listing-thumb']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source",self.external_source)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        title = response.xpath("//div[@class='page-title']/h1/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ',title.strip()))
        property_type=response.xpath("//div[@class='page-title']/h1/text()").get()
        if property_type:
            if "maison" in property_type.lower():
                item_loader.add_value("property_type","house")
            if "appartement"==property_type.lower():
                item_loader.add_value("property_type","apartment")
            if "studio" in property_type.lower():
                item_loader.add_value("property_type","studio")
        description="".join(response.xpath("//div[@class='wpb_text_column wpb_content_element']/p/text()").getall())
        if description:
            item_loader.add_value("description",description)
        property_check=item_loader.get_output_value("property_type")
        if not property_check:
            property_type="".join(response.xpath("//div[@class='wpb_text_column wpb_content_element']/p/text()").getall())
            if property_type:
                if "appartement" in property_type.lower() or "résidence" in property_type.lower():
                    item_loader.add_value("property_type","apartment")
                if "maison" in property_type.lower():
                    item_loader.add_value("property_type","house")
        adres = response.xpath("//div[@class='page-title']/h1/text()").get()
        if adres:
            item_loader.add_value("address", re.sub('\s{2,}', ' ',adres.strip().split(" ")[1]))
        rent=response.xpath("//li[@class='item-price']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[0].replace(" ",""))
        item_loader.add_value("currency","EUR")
        external_id=response.xpath("//div[@class='wpb_wrapper reference-bien']/strong/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id)
        deposit=response.xpath("//p[contains(.,'Dépôt de garantie')]/text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit.split(":")[-1].split("€")[0])
        utilities=response.xpath("//p[contains(.,'charges')]/text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split("dont")[-1].split("€")[0].strip())
        images=[x for x in response.xpath("//div[@id='property-gallery-js']//div/@data-thumb").getall()]
        if images:
            item_loader.add_value("images",images)
        square_meters=response.xpath("//strong[.='Surface habitable :']/following-sibling::text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m²")[0].split(",")[0].strip())
        room_count=response.xpath("//strong[.='Nombre de pièces :']/following-sibling::text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.strip())
        bathroom_count=response.xpath("//strong[.='Nombre de salles de bain :']/following-sibling::text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.strip())
        latitude=response.xpath("//script[contains(.,'lng')]/text()").get()
        if latitude:
            item_loader.add_value("latitude",latitude.split('"lat":')[1].split(",")[0].replace('"',""))
        longitude=response.xpath("//script[contains(.,'lng')]/text()").get()
        if longitude:
            item_loader.add_value("longitude",longitude.split('"lng":')[1].split(",")[0].replace('"',""))
        floor=response.xpath("//strong[.='Étage :']/following-sibling::text()").get()
        if floor:
            item_loader.add_value("floor",floor.strip())

        yield item_loader.load_item()  