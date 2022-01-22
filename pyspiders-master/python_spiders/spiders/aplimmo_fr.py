# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.http import headers
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request, FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json


class MySpider(Spider):
    name = 'aplimmo_fr'
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    external_source = "Aplimmo_PySpider_france"
    start_urls = ['https://aplimmo.fr/fr/recherche']  # LEVEL 1

    formdata = {
        "search-form-79409[search][category]": "Location|2",
        "search-form-79409[search][type][]": "Appartement|1",
        "search-form-79409[search][city]": "",
        "search-form-79409[search][price_min]": "",
        "search-form-79409[search][price_max]": "",
        "search-form-79409[submit]": "",
        "search-form-79409[search][order]": "",
    }

    headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36"
    }

    def start_requests(self):

        start_urls = [
            {
                "url": [
                    "Appartement|1",
                ],
                "property_type": "apartment"
            },
            {
                "url": [
                    "Maison|2"
                ],
                "property_type": "house"
            },
        ]  # LEVEL 1

        for url in start_urls:
            for item in url.get('url'):
                self.formdata["search-form-79409[search][type][]"] = item
                yield FormRequest(
                    url=self.start_urls[0],
                    dont_filter=True,
                    formdata=self.formdata,
                    headers=self.headers,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get('page', 2)

        seen = False
        for item in response.xpath("//ul[@class='_list listing']/li//figure//@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen = True

        if page == 2 or seen:
            url = f"https://www.era.be/nl/te-huur?page={page}"
            yield Request(url, callback=self.parse, meta={"page": page+1, "property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value(
            "property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        title = "".join(response.xpath("//title//text()").get())
        if title:
            title = title.replace("\u00e9", "").replace(
                "\u00e8", "").replace("\u00e0", "").replace("\u00c9", "")
            item_loader.add_value("title", title)

        external_id = response.xpath("//div[contains(@class,'module module-79420 module-property-info property-info-template-2 ')]//p[contains(.,'Réf. ')]//text()").get()
        if external_id:
            external_id=external_id.split("Réf.")[1]
            item_loader.add_value("external_id",external_id)

        address = "".join(response.xpath("(//div[contains(@class,'module module-79420 module-property-info property-info-template-2 ')]//h1//text())[2]").getall())
        if address:
            item_loader.add_value("address",address.strip())

        city = "".join(response.xpath("(//div[contains(@class,'module module-79420 module-property-info property-info-template-2 ')]//h1//text())[2]").getall())
        if city:
            item_loader.add_value("city",city.strip())

        room_count = response.xpath("//div[contains(@class,'module module-79420 module-property-info property-info-template-2 ')]//p[contains(.,'pièces')]//text()").get()
        if room_count:
            room_count=room_count.split("pièces")[0]
            item_loader.add_value("room_count",room_count)

        square_meters = response.xpath("//div[contains(@class,'module module-79420 module-property-info property-info-template-2 ')]//p[contains(.,'m²')]//text()").get()
        if square_meters:
            square_meters=square_meters.split("m²")[0]
            item_loader.add_value("square_meters",int(float(square_meters)))

        description = response.xpath(
            "//p[contains(@id,'description')]//text()").get()
        if description:
            description = description.replace("\u00e9", "").replace("\u00f4", "").replace("\u20ac", "").replace("\u00e8", "").replace(
                "\u00e0", "").replace("\u00b2", "")
            item_loader.add_value("description", description)

        rent = response.xpath("//div[contains(@class,'module module-79420 module-property-info property-info-template-2 ')]//p[contains(.,'€')]//text()").get()
        if rent:
            rent=rent.split("€")[0]
            item_loader.add_value("rent",rent)
        item_loader.add_value("currency","EUR")


        deposit = response.xpath("//div[contains(@class,'module module-79428 module-property-info property-info-template-7')]//li[contains(.,'Dépôt de garantie')]//span//text()").get()
        if deposit:
            deposit=deposit.split("€")[0]
            item_loader.add_value("deposit",deposit)

        utilities = response.xpath("//ul//li[contains(.,' Provision sur charges récupérables')]//span//text()").get()
        if utilities:
            utilities=utilities.split("€")[0]
            item_loader.add_value("utilities",utilities)

        images = [response.urljoin(x)for x in response.xpath("//div[contains(@class,'slider')]//img[contains(@class,'picture ')]//@src").getall()]
        if images:
                item_loader.add_value("images",images)

        item_loader.add_value("landlord_phone", "+33 4 26 79 01 40")
        item_loader.add_value("landlord_email", "contact@aplimmo.fr")
        item_loader.add_value("landlord_name", "APLIMMO")

        yield item_loader.load_item()



    