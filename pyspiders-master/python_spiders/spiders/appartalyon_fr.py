# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser

class MySpider(Spider):
    name = 'appartalyon_fr_disable'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.groupe-appart-immo.com/catalog/advanced_search_result.php?action=update_search&search_id=1691768830962706&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=1&C_27_tmp=1&C_33_search=COMPRIS&C_33_type=NUMBER&C_33_MIN=&C_33_MAX=&C_30_MIN=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MAX=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.groupe-appart-immo.com/catalog/advanced_search_result.php?action=update_search&search_id=1691768830962706&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=2%2CMaisonArchitecte&C_27_tmp=2&C_27_tmp=MaisonArchitecte&C_33_search=COMPRIS&C_33_type=NUMBER&C_33_MIN=&C_33_MAX=&C_30_MIN=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MAX=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[@class='titreBien']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_page = response.xpath("//a[@class='page_suivante']/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        item_loader.add_value("external_source", "Appartalyon_PySpider_france")       
        item_loader.add_xpath("title", "//h1/text()")
        item_loader.add_xpath("zipcode", "//div[div[.='Code postal : ']]/div[2]/text()")
        item_loader.add_xpath("city", "//div[div[.='Ville : ']]/div[2]/text()")
        item_loader.add_xpath("address", "//span[@class='alur_location_ville']//text()")
        item_loader.add_xpath("floor", "//div[div[.='Etage : ']]/div[2]/text()")
        item_loader.add_xpath("deposit", "//div[div[.='Dépôt de Garantie : ']]/div[2]/text()")
        item_loader.add_xpath("utilities", "//div[div[.='Provision sur charges : ']]/div[2]/text()")
        item_loader.add_xpath("energy_label", "//div[div[.='Conso Energ : ']]/div[2]/text()")
        item_loader.add_xpath("external_id", "substring-after(//li/span[contains(.,'Ref. : ')],': ')")
        room_count = response.xpath("//div[div[.='Chambres : ']]/div[2]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            item_loader.add_xpath("room_count", "//div[div[.='Nombre pièces : ']]/div[2]/text()")
        bathroom_count = response.xpath("//div[@class='infos-bien'][div[contains(.,'Salle(s) d')]]/div[2]/text()[.!='0']").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
    
        square_meters = response.xpath("//div[div[.='Surface : ']]/div[2]/text()").get()
        if square_meters:
            square_meters = square_meters.split("m")[0].strip()
            item_loader.add_value("square_meters", int(float(square_meters.split("m")[0].strip().replace(",","."))))

        furnished = response.xpath("//div[div[.='Meublé : ']]/div[2]/text()").get()
        if furnished:
            if "non" in furnished.lower():
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)

        terrace = response.xpath("//div[div[.='Nombre de terrasses : ']]/div[2]/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        balcony = response.xpath("//div[div[.='Nombre balcons : ']]/div[2]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        elevator = response.xpath("//div[div[.='Ascenseur : ']]/div[2]/text()").get()
        if elevator:
            if "non" in elevator.lower():
                item_loader.add_value("elevator", False)
            else:
                item_loader.add_value("elevator", True)
        available_date = response.xpath("//div[div[.='Disponibilité : ']]/div[2]/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
     
        script_map = response.xpath("//script[contains(.,'position: new google.maps.LatLng(')]/text()").get()
        if script_map:
            latlng = script_map.split("position: new google.maps.LatLng(")[1].split(")")[0].strip()
            item_loader.add_value("latitude", latlng.split(",")[0].strip())
            item_loader.add_value("longitude", latlng.split(",")[1].strip())
        description = " ".join(response.xpath("//div[@class='product-desc']/text()").getall()) 
        if description:
            item_loader.add_value("description", description.strip())
     
        images = [response.urljoin(x) for x in response.xpath("//ul[@class='liste-photos']/li/a/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
 
        rent = response.xpath("//div[contains(@class,'product-pictos')]//span[@class='alur_loyer_price']/text()").get()
        if rent:
            rent = rent.split("oyer")[1].split("€")[0].strip().replace(" ", "").split(".")[0].split(",")[0]
            item_loader.add_value("rent", rent.strip().replace("\xa0", ""))
            item_loader.add_value("currency", "EUR")

        item_loader.add_value("landlord_name", "Groupe Appart Immo")
        item_loader.add_value("landlord_phone", "04 78 41 41 66")
        yield item_loader.load_item()