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
    name = 'immobiliareristori_it'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "Immobiliareristori_Pyspider_italy"
    

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.immobiliareristori.it/elenco_immobili_f.asp?idm=6652&idcau=2",
                ],
                "property_type": "apartment"
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
        
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//div[@class='holder row']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen = True
        
        if page == 2 or seen:
            url = f"https://www.immobiliareristori.it/elenco_immobili_f.asp?start=9&ordinaper=D&idcau={page}&fascia_mq=0&inv=#elenco_imm"
            yield Request(url, callback=self.parse, meta={"page": page+1, "property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        external_id=response.xpath("//h2[contains(@class,'prop-title')]//text()").get()
        if external_id:
            external_id="".join(external_id.split("Rif:")[1].split("-")[0].replace("\u00a0",""))
            item_loader.add_value("external_id",external_id)

        title=response.xpath("//h2[contains(@class,'prop-title')]//text()").get()
        if title:
            item_loader.add_value("title",title.replace("\u00a0",""))

        city=response.xpath("//h2[contains(@class,'prop-title')]//text()").get()
        if city:
            city="".join(city.split(" ")[-2:-1])
            item_loader.add_value("city",city)

        address=response.xpath("//h5[contains(@class,'zona_scheda')]//text()").get()
        if address:
            item_loader.add_value("address",address)

        rent=response.xpath("//span[contains(@class,'prop-price')]//text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€.")[1])
        item_loader.add_value("currency","EUR")

        square_meters=response.xpath("//li[contains(@class,'info-label ')]//span[contains(.,'Superficie:')]//following-sibling::span//text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("mq. ")[1])

        room_count=response.xpath("//li[contains(@class,'info-label ')]//span[contains(.,'Num. Vani:')]//following-sibling::span//text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        else:
            room_count=response.xpath("//li[contains(@class,'info-label ')]//span[contains(.,'Piani Totali:')]//following-sibling::span//text()").get()
            if room_count:
                item_loader.add_value("room_count",room_count)

        # balcony=response.xpath("//li[contains(@class,'info-label ')]//span[contains(.,'Balcone:')]//following-sibling::span//text()").get()
        # if 'NO' in balcony.lower():
        #     item_loader.add_value("balcony",False)
        # else:
        #     item_loader.add_value("balcony",True)

        # terrace=response.xpath("//li[contains(@class,'info-label ')]//span[contains(.,'Terrazzo:')]//following-sibling::span//text()").get()
        # if 'SI' in terrace.lower():
        #     item_loader.add_value("terrace",True)
        # else:
        #     item_loader.add_value("terrace",False)

        description=response.xpath("//p[@class='p-description']//text()").getall()
        if description:
            item_loader.add_value("description",description)

        energy_label=response.xpath("//div[@class='classe-energetica']//div[contains(.,'Classe Energetica')]//following-sibling::span//text()").get()
        if energy_label:
            item_loader.add_value("energy_label",energy_label)

        images = [response.urljoin(x)for x in response.xpath("//img[@class='media-object']//@src").extract()]
        if images:
                item_loader.add_value("images", images)


        latitude_longitude = response.xpath("//script[contains(@type,'text/javascript')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split("maps.LatLng('")[-1].split("',")[0]
            longitude = latitude_longitude.split("maps.LatLng('")[-1].split(",")[1].split("');")[0]
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_phone", "0556800250")
        item_loader.add_value("landlord_email", "info@immobiliareristori.it")
        item_loader.add_value("landlord_name", "INVIA UN MESSAGGIO")







        yield item_loader.load_item()