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
    name = 'agenceeuropeenne_com'
    external_source = "Agenceeuropeenne_PySpider_france"
    post_urls = ['http://www.agenceeuropeenne.com/recherche/']  # LEVEL 1

    formdata = {
        "data[Search][offredem]": "2",
        "data[Search][idtype][]": "1",
        "data[Search][prixmax]": "",
        "data[Search][piecesmin]": "",
        "data[Search][NO_DOSSIER]": "",
        "data[Search][distance_idvillecode]": "",
        "data[Search][prixmin]": "",
        "data[Search][surfmin]": "",
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36"
    }

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "2",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "1"
                ],
                "property_type": "house"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                self.formdata["data[Search][idtype][]"] = item
                yield FormRequest(
                    url=self.post_urls[0],
                    callback=self.parse,
                    dont_filter=True,
                    formdata=self.formdata,
                    headers=self.headers,
                    meta={
                        'property_type': url.get('property_type'),
                        'type': item
                    }
                )

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//a[contains(.,'voir')]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen = True
        
        if page == 2 or seen:
            url = f"http://www.agenceeuropeenne.com/recherche/{page}"
            self.formdata["data[Search][idtype][]"] = response.meta.get('type')
            yield FormRequest(
                url=url,
                callback=self.parse,
                dont_filter=True,
                formdata=self.formdata,
                headers=self.headers,
                meta={'property_type': response.meta.get('property_type'), "page":page+1, 'type': response.meta.get('type')}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        external_id=response.xpath("//span[@itemprop='productID']/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.split("Ref")[0])
        
        rent=response.xpath("//span[.='Prix']/following-sibling::text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[0].strip())
        title=response.xpath("//div[@class='bienTitle']/h2/text()").get()
        if title:
            item_loader.add_value("title",title.replace("  ",""))

        adres=response.xpath("//div[@class='bienTitle']/h2/text()").get()
        if adres:
            item_loader.add_value("address",adres.split("-")[-1])
        city=response.xpath("//span[.='Ville']/following-sibling::span/text()").get()
        if city:
            item_loader.add_value("city",city.replace("  ","").replace("\n",""))
        zipcode=response.xpath("//span[.='Code postal']/following-sibling::span/text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.replace("  ","").replace("\n",""))
        square_meters=response.xpath("//span[.='Surface habitable (m²)']/following-sibling::span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m")[0])
        room_count=response.xpath("//span[.='Nombre de pièces']/following-sibling::span/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        desc=response.xpath("//p[@itemprop='description']/text()").get()
        if desc:
            item_loader.add_value("description",desc)
        images=[x for x in response.xpath("//figure[@class='mainImg ']//img//@src").getall()]
        if images:
            item_loader.add_value("images",images)
        furnished=response.xpath("//span[.='Meublé']/following-sibling::span/text()").get()
        if furnished and "oui" in furnished.lower():
            item_loader.add_value("furnished",True)
        elevator=response.xpath("//span[.='Ascenseur']/following-sibling::span/text()").get()
        if elevator and "oui" in elevator.lower():
            item_loader.add_value("elevator",True)
        floor=response.xpath("//span[.='Etage']/following-sibling::span/text()").get()
        if floor:
            item_loader.add_value("floor",floor)
        latitude=response.xpath("//script[contains(.,'Map.setCenter')]/text()").get()
        if latitude:
            item_loader.add_value("latitude",latitude.split("Map.setCenter")[-1].split("Map")[0].split("lat")[-1].split(",")[0].replace(":","").strip())

        longitude=response.xpath("//script[contains(.,'Map.setCenter')]/text()").get()
        if longitude:
            item_loader.add_value("longitude",longitude.split("Map.setCenter")[-1].split("Map")[0].split("lng")[-1].split("}")[0].replace(":","").strip())
        yield item_loader.load_item()