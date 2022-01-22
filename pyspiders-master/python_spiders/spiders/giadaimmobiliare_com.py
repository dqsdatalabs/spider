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
    name = 'giadaimmobiliare_com'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "Giadaimmobiliare_PySpider_italy"

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.giadaimmobiliare.com/property-search/?status=affitti&type=appartamenti",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://www.giadaimmobiliare.com/property-search/?status=affitti&type=attico",
                    "https://www.giadaimmobiliare.com/property-search/?status=affitti&type=casale",
                    "https://www.giadaimmobiliare.com/property-search/?status=affitti&type=loft",
                    "https://www.giadaimmobiliare.com/property-search/?status=affitti&type=ville"
                ],
                "property_type": "house"
            },
            {
                "url": [
                    "https://www.giadaimmobiliare.com/property-search/?status=affitti&type=monolocale"
                ],
                "property_type": "studio"
            },
            {
                "url": [
                    "https://www.giadaimmobiliare.com/property-search/?status=affitti&type=camera-in-appartamento",
                    "https://www.giadaimmobiliare.com/property-search/?status=affitti&type=stanza"
                ],
                "property_type": "room"
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
        
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//h4/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen = True
        
        if page == 2 or seen:
            url = f"https://www.giadaimmobiliare.com/property-search/page/{page}/?status=affitti&type={response.url.split('type=')[-1]}"
            yield Request(url, callback=self.parse, meta={"page": page+1, "property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)

        external_id="".join(response.xpath("//link[@rel='shortlink']//@href").get())
        if external_id:
            external_id="".join(external_id.split("=")[1])
            item_loader.add_value("external_id",external_id)

        city=response.xpath("(//nav[@class='property-breadcrumbs']//ul//li//a//text())[2]").get()
        if city:
            item_loader.add_value("city",city)
            item_loader.add_value("address",city)

        rent=response.xpath("//h5//span[contains(.,'al mese')]//text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[1].split("al")[0])
        item_loader.add_value("currency","EUR")

        square_meters=response.xpath("//span//text()[contains(.,'mq')]").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("mq")[0])

        room_count=response.xpath("//span//text()[contains(.,'Locali')]").get()
        if room_count:
            item_loader.add_value("room_count",room_count.split("Locali")[0])

        bathroom_count=response.xpath("//span//text()[contains(.,'Bagno/i')]").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.split("Bagno/i")[0])
        else:
            bathroom_count=response.xpath("(//div[@class='property-meta clearfix']//span[contains(.,'Bagno')]//text())[8]").get()
            if bathroom_count:
                item_loader.add_value("bathroom_count",bathroom_count.split("Bagno")[0])

        description=response.xpath("//div[@class='content clearfix']//p//text()").getall()
        if description:
            item_loader.add_value("description",description)

        furnished=response.xpath("//ul[@class='arrow-bullet-list clearfix']//li[contains(.,'arredato')]//text()").get()
        if furnished:
            item_loader.add_value("furnished",True)
        else:
            item_loader.add_value("furnished",False)

        images = [response.urljoin(x)for x in response.xpath("//a[contains(@data-rel,'prettyPhoto')]//img//@src").extract()]
        if images:
                item_loader.add_value("images", images)

        latitude_longitude = response.xpath(
            "//script[contains(.,'lang')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split(
                '{"lat":"')[1].split('",')[0]
            longitude = latitude_longitude.split(
                ',"lang":"')[1].split('",')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_phone", "338 3735170 - 328 6867945")
        item_loader.add_value("landlord_email", "info@giadaimmobiliare.com")
        item_loader.add_value("landlord_name", "Giada Immobiliare")
        yield item_loader.load_item()