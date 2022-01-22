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
    name = 'weelodge_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = "Weelodge_PySpider_france"


    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.weelodge.fr/louer/liste?type_bien%5B%5D=Appartement&budget_min=selectionner&budget_max=selectionner&surface_min=selectionner&surface_max=selectionner&surface_terrain_min=selectionner&surface_terrain_max=selectionner&annee_construction_min=selectionner&annee_construction_max=selectionner&ref=&search=Rechercher",
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

        for item in response.xpath("//div[@class='list_vignet']//a[@class='cta-fav']//following-sibling::a//@href").extract():
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
            item_loader.add_value("title",title.replace("\u00b2","").replace("\u00c9",""))
             
        description=response.xpath("//div[@class='detail_desc']//text()").getall()
        if description:
            item_loader.add_value("description",description)

        external_id=response.xpath("//ul[@class='detail_value']//li[@class='ref']//text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id)

        room_count=response.xpath("//ul[@class='detail_value']//li[2]//text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        else:
            room_count=response.xpath("//ul[@class='detail_value']//li[3]//text()").get()
            if room_count:
                item_loader.add_value("room_count",room_count)

        square_meters=response.xpath("//ul[@class='detail_value']//li[contains(.,'m')][1]//text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m")[0])

        rent="".join(response.xpath("//div[@class='detail_desc']//p[contains(@class,'price')]//text()").get())
        if  rent:
            rent = rent.replace(" ","").split("€")[0].strip()
            item_loader.add_value("rent",rent)
        item_loader.add_value("currency","EUR")

        images=[response.urljoin(x) for x in response.xpath("//ul[@class='filmstrip']//li//img//@src").getall()]
        if images:
            item_loader.add_value("images",images)
            
        landlord_phone=response.xpath("//span[contains(.,'Weelodge')]//following-sibling::strong//text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_name", "Weelodge")
        yield item_loader.load_item()