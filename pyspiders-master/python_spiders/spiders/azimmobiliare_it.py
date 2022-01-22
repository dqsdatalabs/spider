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
    name = 'azimmobiliare_it'
    execution_type='testing'
    country='italy'
    locale='it' 
    external_source = "Azimmobiliare_PySpider_italy"

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.azimmobiliare.it/appartamenti-affitto/",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://www.azimmobiliare.it/attici-mansarde-affitto/",
                    "https://www.azimmobiliare.it/ville-affitto/"
                ],
                "property_type": "house"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type'), "base_url": item}
                )

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        base_url = response.meta.get('base_url')
        seen = False
        for item in response.xpath("//a[contains(.,'Dettaglio ')]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen = True
        
        if page == 2 or seen:
            url = f"{base_url}?tags3_1=&tags_control=&pg={page}"
            yield Request(url, callback=self.parse, meta={"page": page+1, "property_type": response.meta.get('property_type'), "base_url": base_url})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)


        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)

        external_id=response.xpath("//title//text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.split("Rif.")[1].split(" ")[0])

        rent=response.xpath("//span[@title='Prezzo']//following-sibling::b//text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[1])
        item_loader.add_value("currency","EUR")

        address=response.xpath("//span[@class='location'][1]//strong//text()").get()
        if address:
            item_loader.add_value("address",address)

        desc=response.xpath("//p[@class='description']//text()").get()
        if desc:
            item_loader.add_value("description",desc)

        utilities=response.xpath("//span[@title='Spese Condominiali Mensili']//following-sibling::b//text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split("€")[1])

        room_count=response.xpath("//span[@title='Locali']//following-sibling::b//text()").get()
        if room_count and "." in room_count:
            room_count = room_count.split('.')[0].strip()
            if room_count:
                item_loader.add_value("room_count", (int(room_count)+1))
        else:
            item_loader.add_value("room_count", room_count)

        bathroom_count=response.xpath("//span[@title='Bagni']//following-sibling::b//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)

        square_meters=response.xpath("//span[@title='MQ']//following-sibling::b//text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters)


        energy_label=response.xpath("//span[@title='Classe Energ.']//following-sibling::b//text()").get()
        if energy_label:
            item_loader.add_value("energy_label",energy_label)

        images = [response.urljoin(x) for x in response.xpath("//a[@class='swipebox small wow fadeIn']//@href").getall()]
        if images:
            item_loader.add_value("images", images)

        furnished = response.xpath("//span[@title='Arredato']/parent::li/b/span[@class='presence']").get()
        if furnished:
            item_loader.add_value("furnished", True)

        parking = response.xpath("//span[contains(@title,'Parcheggio')]/parent::li/b/span[@class='presence']").get()
        if parking:
            item_loader.add_value("parking", True)

        latitude_longitude=response.xpath("//script[contains(.,'LatLng')]//text()").get()
        if latitude_longitude:
            lat=latitude_longitude.split('google.maps.LatLng(')[1].split(',')[0]
            lng=latitude_longitude.split('google.maps.LatLng(')[1].split(',')[1].split(");")[0]
            item_loader.add_value("latitude",lat)
            item_loader.add_value("longitude", lng)

        landlord_name = response.xpath("//span[@class='agenzia-nome']/text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)
        else:
            item_loader.add_value("landlord_name", "Az Immobiliare")
        landlord_phone = response.xpath("(//span[@class='agenzia-tel']/tel/a/text())[2]").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone.strip())

        item_loader.add_value("landlord_email", "info@azimmobiliare.it")

        yield item_loader.load_item()