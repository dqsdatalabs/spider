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
    name = 'vendmy_com'
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    external_source = "Vendmy_PySpider_france"

    def start_requests(self): 
        start_urls = [
            {
                "url": [
                    "https://vendmy.com/rechercher-une-propriete/?statut=43&type=30&price=&orderby=featured",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://vendmy.com/rechercher-une-propriete/?statut=43&type=40&price=&orderby=featured",
                    "https://vendmy.com/rechercher-une-propriete/?statut=43&type=444&price=&orderby=featured",
                    "https://vendmy.com/rechercher-une-propriete/?statut=43&type=442&price=&orderby=featured",
                    "https://vendmy.com/rechercher-une-propriete/?statut=43&type=111&price=&orderby=featured",
                    "https://vendmy.com/rechercher-une-propriete/?statut=43&type=112&price=&orderby=featured",
                    "https://vendmy.com/rechercher-une-propriete/?statut=43&type=114&price=&orderby=featured",
                    "https://vendmy.com/rechercher-une-propriete/?statut=43&type=51&price=&orderby=featured"
                ],
                "property_type": "house"
            },
            {
                "url": [
                    "https://vendmy.com/rechercher-une-propriete/?statut=43&type=113&price=&orderby=featured",
                ],
                "property_type": "studio"
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
        for item in response.xpath("//h2[@class='iw-property-title']//@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_xpath("title","//title//text()")

        external_id=response.xpath("//span[.='ID de propriété']/following-sibling::text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.strip())
        adres=response.xpath("//span[@itemprop='address']/text()").get()
        if adres:
            item_loader.add_value("address",adres)
            city=adres.split(",")[-2].split(" ")[-1]
            if city:
                item_loader.add_value("city",city)
            zipcode=adres.split(",")[-2].strip().split(" ")[0]
            if zipcode:
                item_loader.add_value("zipcode",zipcode)

        rent=response.xpath("//span[.='Prix:']/following-sibling::text()").get()
        if rent and  not "Appelez" in rent:
            item_loader.add_value("rent",rent.replace(",","").split("€")[0].strip())
        item_loader.add_value("currency","EUR")
        
        utilities=response.xpath("//span[.='Charges / mois:']/following-sibling::text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split("€")[0])
        room_count=response.xpath("//i[@class='reality-icon-bedroom']/following-sibling::text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.split("Ch")[0])
        bathroom_count=response.xpath("//i[@class='reality-icon-bathroom']/following-sibling::text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.split("Sd")[0])
        square_meters=response.xpath("//i[@class='reality-icon-area']/following-sibling::text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split(".")[0].split("m²")[0])
        desc=" ".join(response.xpath("//div[@class='iwp-single-property-description']//p//text()").getall())
        if desc:
            item_loader.add_value("description",desc)
        images=response.xpath("//li//figure//a//@href").getall()
        if images:
            item_loader.add_value("images",images)
        features=response.xpath("//span[.='Prestations / Services:']/parent::h3/following-sibling::div/ul//li//text()").getall()
        for i in features:
            if "Ascenseur" in i:
                item_loader.add_value("elevator",True)
            if "Balcon" in i:
                item_loader.add_value("balcony",True)
        latitude=response.xpath("//script[contains(.,'lng')]/text()").get()
        if latitude:
            item_loader.add_value("latitude",latitude.split("zoom")[0].split("lat:")[-1].split(",")[0].strip())
        longitude=response.xpath("//script[contains(.,'lng')]/text()").get()
        if longitude:
            item_loader.add_value("longitude",longitude.split("zoom")[0].split("lng:")[-1].split("}")[0].strip())
        item_loader.add_value("landlord_name","VENDMY")



        yield item_loader.load_item()