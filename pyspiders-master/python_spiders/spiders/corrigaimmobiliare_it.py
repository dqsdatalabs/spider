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
    name = 'corrigaimmobiliare_it'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "Corrigaimmobiliare_Pyspider_italy"

    # LEVEL 1
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.corrigaimmobiliare.it/immobili.php?contratto=2&comune=&categoria=1&tipologia=&prezzo=",
                ],
                "property_type": "apartment"
            },
            {
                "url": [
                    "https://www.corrigaimmobiliare.it/immobili.php?contratto=2&comune=&categoria=8&tipologia=&prezzo=",
                ],
                "property_type": "house"
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

        for item in response.xpath("//a[@class='immagine']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        external_id=response.xpath("//p[contains(.,'Rif')]/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.split(".")[-1].strip())
        else:
            external_id = response.xpath("//p[@id='codice']/strong/text()").get()
            if external_id:
                item_loader.add_value("external_id",external_id.split()[-1].strip())
        item_loader.add_xpath("title","//title//text()") 
        adres=response.xpath("//p[@id='luogo']/strong/text()").get()
        if adres:
            item_loader.add_value("address",adres)
            item_loader.add_value("city",adres.split("-")[0].strip())
        
        rent=response.xpath("//p[@id='prezzo']/strong/text()").get()
        if rent:
            rent=rent.split("Euro")[0].replace(".","").strip()
            item_loader.add_value("rent",rent)
        item_loader.add_value("currency","EUR")
        square_meters=response.xpath("//p[contains(.,'MQ')]//strong/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters)
        bathroom_count=response.xpath("//p[contains(.,'Bagni')]//strong/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        energy_label=response.xpath("//p[contains(.,'Classe energetica')]//strong/text()").get()
        if energy_label:
            item_loader.add_value("energy_label",energy_label)
        desc=response.xpath("//p[@id='descrizione']/text()").get()
        if desc:
            item_loader.add_value("description",desc)
        images=[x for x in response.xpath("//div[@class='thumbs']/a/img/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        item_loader.add_value("landlord_name","Corriga Immobiliare")
        item_loader.add_value("landlord_email","corrigaimmobiliaresrl@gmail.com")
        item_loader.add_value("landlord_phone","070-8488086")
        yield item_loader.load_item()