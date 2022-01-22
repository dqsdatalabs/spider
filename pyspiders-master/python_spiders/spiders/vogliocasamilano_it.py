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
    name = 'vogliocasamilano_it'
    execution_type='testing'
    country='italy'
    locale='it'
    external_source = 'Vogliocasamilano_PySpider_italy'

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://vogliocasamilano.it/?address=&radius=Ricerca+entro+km&contratto=Affitto&comune=Comune&tipologia=Appartamento&bedrooms=Stanze&bathrooms=Bagni&min_price=0&max_price=1500000",
                    "https://vogliocasamilano.it/?address=&radius=Ricerca+entro+km&contratto=Affitto&comune=Comune&tipologia=Attico&bedrooms=Stanze&bathrooms=Bagni&min_price=0&max_price=1500000"
                ],
                "property_type" : "apartment"
            }
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

        for item in response.xpath("//div[@class='col-lg-7 col-md-7 col-sm-12 col-xs-12 property-content ']/h1/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta['property_type']})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        title = response.xpath("//div[@class='pull-left']/h3/text()").get()
        if title:
            item_loader.add_value("title", title)
        
        external_id = response.url
        if external_id:
            item_loader.add_value("external_id", external_id.split('dettaglio/')[-1])
        city=response.xpath("//div[@class='pull-left']/p/i/following-sibling::text()").get()
        if city:
            item_loader.add_value("city",city.split(" ")[1])


        rent = response.xpath("//div[@class='pull-right']/h3/span/text()").get()
        if rent:
            item_loader.add_value("rent", rent.split('€')[-1].replace(".","").strip())
        item_loader.add_value("currency","EUR")

        square_meters = response.xpath("//div[@class='floor-plans']/table//tr[2]/td[1]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters)

        room_count = response.xpath("//div[@class='floor-plans']/table//tr[2]/td[2]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//div[@class='floor-plans']/table//tr[2]/td[3]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        address = "".join(response.xpath("//div[@class='pull-left']/p/text()").getall())
        if address:
            item_loader.add_value("address", address.split('via')[-1].strip())

        parking = response.xpath("//div[@class='col-lg-4 col-md-4 col-sm-4 col-xs-12']/ul/li/text()[contains(.,'Box Auto')]").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//div[@class='col-lg-4 col-md-4 col-sm-4 col-xs-12']/ul/li/text()[contains(.,'Balcon')]").get()
        if balcony:
            item_loader.add_value("balcony", True)

        desc = "".join(response.xpath("//div[@class='main-title-2'][contains(.,'Descrizione')]/parent::div/p/text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
        furnished=item_loader.get_output_value("description")
        if furnished and "arredato" in furnished:
            item_loader.add_value("furnished",True)

        latlng = response.xpath("//script/text()[contains(.,'LoadMap')]").get()
        if latlng:
            latitude = latlng.split('defaultLat =')[-1].split(';')[0].strip()
            item_loader.add_value("latitude", latitude)
            longitude = latlng.split('defaultLng =')[-1].split(';')[0].strip()
            item_loader.add_value("longitude", longitude)

        images = [x for x in response.xpath("//div[@class='carousel-inner']/div/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))

        landlord_name = response.xpath("//div[@class='main-title-2']/following-sibling::ul//li/text()[contains(.,'Indirizzo')]").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name.split('Via')[-1].split('via')[-1].strip())
        landlord_email = response.xpath("//div[@class='main-title-2']/following-sibling::ul//li/a/text()[contains(.,'@')]").get()
        if landlord_email:
            item_loader.add_value("landlord_email", landlord_email.strip())
        landlord_phone = response.xpath("//div[@class='main-title-2']/following-sibling::ul//li[contains(.,'Telefono')]/a/text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone.strip())

        yield item_loader.load_item()