# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request, FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser


class MySpider(Spider):
    name = "avimmo_fr"
    execution_type = "testing"
    country = "france"
    locale = "fr"

    def start_requests(self):

        headers = {
            "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
            "origin": "https://www.a-vimmo.fr",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.102 YaBrowser/20.9.3.126 Yowser/2.5 Safari/537.36",
            "x-requested-with": "XMLHttpRequest",
        }

        url = "https://www.a-vimmo.fr/wp-admin/admin-ajax.php"
        urls = [
            {
                "url": "https://www.a-vimmo.fr/wp-json/facetwp/v1/refresh",
                "property_type": "apartment",
                "type": "appartement",
            },
            {
                "url": "https://www.a-vimmo.fr/wp-json/facetwp/v1/refresh",
                "property_type": "house",
                "type": "maison",
            },
        ]  # LEVEL 1
        for url in urls:

            data = {
                "action": "facetwp_refresh",
                "data[facets]": {
                    "acheter_louer": ["4"],
                    "type_de_bien": ["appartement"],
                },
                "data[static_facet]": "",
                "data[http_params][uri]": "transaction/location",
                "data[template]": "location",
                "data[extras][pager]": "true",
                "data[soft_refresh]": "0",
                "data[paged]": "1",
            }
            data["data[facets]"]["type_de_bien"][0] = url.get("type")
            yield FormRequest(
                url=url.get("url"),
                formdata=data,
                dont_filter=True,
                headers=headers,
                callback=self.parse,
                meta={
                    "property_type": url.get("property_type"),
                },
            )

    # 1. FOLLOWING
    def parse(self, response):

        data = json.loads(response.body)
        data_json = data.get("template")
        sel = Selector(text=data_json, type="html")
        for item in sel.xpath("//div[@class='item']//a/@href").extract():
            yield Request(
                item,
                callback=self.populate_item,
                meta={"property_type": response.meta.get("property_type")},
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        non_house = response.xpath("//h1/text()").get()
        if "bar" in non_house.lower() or 'restaurant' in non_house.lower():
            return
        property_type = response.meta.get("property_type")
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("external_link", response.url)
        externalid=response.xpath("//span[.='Référence']/following-sibling::strong/text()").get()
        if externalid:
            item_loader.add_value("external_id",externalid)
        item_loader.add_value("external_source", "Avimmo_PySpider_" + self.country + "_" + self.locale)
        available = response.xpath("//span[contains(@class,'etat-dispo')]//text()").extract_first()
        if available:
            item_loader.add_xpath("title", "//h1/text()")
            
            square_meters = response.xpath("//ul/li[contains(.,'Surface')]/strong/text()").extract_first()
            if square_meters:
                item_loader.add_value("square_meters", square_meters.split("m")[0])

            room_count = response.xpath("//ul/li[contains(.,'pièces')]/strong/text()").extract_first()
            if room_count:
                item_loader.add_value("room_count", room_count)

            bathroom_count = response.xpath("//ul[@class='details']//li[contains(.,'Salle de bain')]//strong//text()").get()
            if bathroom_count:
                item_loader.add_value("bathroom_count", bathroom_count)

            rent = response.xpath("//p[@class='immo-infos']/span[1]/text()[normalize-space()]").get()
            if rent:
                item_loader.add_value("rent", rent.split())
                item_loader.add_value("currency", "EUR")
                
            utilities = response.xpath("//p[@class='immo-infos']/span[1]/small/text()").extract_first()
            if utilities:
                utilities = utilities.split(":")[1].strip()
                item_loader.add_value("utilities", utilities)

            prop = " ".join(response.xpath("//ul/li[contains(.,'Equipements')]/strong/text()").extract())
            if prop: 
                if "terrasse" in prop.lower():
                    item_loader.add_value("terrace", True)
                if "garage" or "parking" in prop.lower():
                    item_loader.add_value("parking", True)
                if "Ascenseur" in prop.lower():
                    item_loader.add_value("elevator", True)

            description = "".join(response.xpath("//div[contains(@class,'the_content')]/p//text()").extract())
            if description:
                item_loader.add_value("description", description)
            utilities= "".join(response.xpath("//div[contains(@class,'the_content')]/p//text()").getall())
            if utilities:
                utilities=utilities.split("Charges:")[-1].split("€")[0].strip()
                if utilities:
                    item_loader.add_value("utilities",utilities)
            deposit= "".join(response.xpath("//div[contains(@class,'the_content')]/p//text()").getall())
            if deposit:
                deposit=deposit.split("Caution:")[-1].split("€")[0].strip()
                if deposit:
                    item_loader.add_value("deposit",deposit)

            latitude_longitude = response.xpath("//a[contains(@rel,'publisher')]//@href").get()
            if latitude_longitude:
                latitude = latitude_longitude.split('/@')[1].split(',')[0]
                longitude = latitude_longitude.split('/@')[1].split(',')[1].split(',')[0].strip()      
                item_loader.add_value("longitude", longitude)
                item_loader.add_value("latitude", latitude)
                

            images = [
                x for x in response.xpath("//div[@class='item-figure']//img/@src").extract()
            ]
            if images:
                item_loader.add_value("images", images)

            address = response.xpath("//p[@class='immo-secteur grid_12']//text()").get()
            if address:
                item_loader.add_value("address", address.strip())
                item_loader.add_value("city", address.strip())

            item_loader.add_value("landlord_name", "A&V Immo")
            item_loader.add_value("landlord_phone", "0389316796")
            item_loader.add_value("landlord_email", "contact@a-vimmo.fr")

            yield item_loader.load_item()
