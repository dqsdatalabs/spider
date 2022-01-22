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
    name = 'agencefranceimmo_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = "Agencefranceimmo_PySpider_france"

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "http://www.agencefranceimmo.com/recherche/",
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
        
        for item in response.xpath("//div[@class='caption-footer col-md-12 col-xs-12 col-sm-4']//a[contains(.,'voir')]//@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)


        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title.replace("\u00e8","").replace("\u00b2",""))

        external_id=response.xpath("//span[@itemprop='productID']//text()").get()
        if external_id:
            external_id=external_id.split("Ref :")[1]
            item_loader.add_value("external_id",external_id)   

        description=response.xpath("//p[@itemprop='description']//text()").getall()
        if description:
            item_loader.add_value("description",description)

        city=response.xpath("//div[@id='infos']//p[contains(.,'Ville ')][1]//span//text()").get()
        if city:
            item_loader.add_value("city",city.replace("\n","").replace(" ",""))

        room_count=response.xpath("//div[@id='infos']//p[contains(.,'Nombre de pièces')]//span//text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        else:
            room_count=response.xpath("//div[@id='infos']//p[contains(.,'Nombre de chambre')]//span//text()").get()
            if room_count:
                item_loader.add_value("room_count",room_count)

        floor=response.xpath("//div[@id='infos']//p[contains(.,'Nombre de niveaux')]//span//text()").get()
        if floor:
            item_loader.add_value("floor",floor)

        square_meters=response.xpath("//div[@id='infos']//p[contains(.,'Surface habitable')]//span//text()").get()
        if square_meters:
            square_meters = square_meters.split("m²")[0].strip()
            item_loader.add_value("square_meters",square_meters)

        rent=response.xpath("//div[@id='infos']//p[contains(.,'Prix du bien')]//span//text()").get()
        if  rent:
            rent = rent.replace(" ","").split("€")[0].strip()
            item_loader.add_value("rent",rent)
        item_loader.add_value("currency","EUR")

        utilities=response.xpath("//div[@id='infosfi']//p[contains(.,'Charges ')]//span//text()").get()
        if  utilities:
            item_loader.add_value("utilities",utilities)

        images=[response.urljoin(x) for x in response.xpath("//ul[@class='imageGallery imageHC  loading']//li//img//@src").getall()]
        if images:
            item_loader.add_value("images",images)

        latitude_longitude = response.xpath("//script[contains(.,'Lng')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split("center: { lat :")[1].split(",")[0]
            longitude = latitude_longitude.split("center: { lat :")[1].split("lng:")[1].split("}")[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)   

        item_loader.add_value("landlord_phone", "01 78 907 907")
        item_loader.add_value("landlord_email", "infomontigny@agencefranceimmo.com")
        item_loader.add_value("landlord_name", "France Immo")

        yield item_loader.load_item()