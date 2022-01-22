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
import re
class MySpider(Spider):
    name = 'agencegalerie_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.agencegalerie.fr/fr/liste.htm?page={}&RgpdConsent=1613732345246&ListeViewBienForm=text&ope=2&filtre=2",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.agencegalerie.fr/fr/liste.htm?page={}&RgpdConsent=1613732345246&ListeViewBienForm=text&ope=2&filtre=1",
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format(1),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "base_url":item})

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)
        max_page = response.xpath("//span[@class='nav-page-position']/text()").get()
        max_page = int(max_page.split('/')[-1].strip()) if max_page else -1

        for item in response.xpath("//a[contains(.,'Plus de détails')]/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        if page <= max_page:
            base_url = response.meta["base_url"]
            p_url = base_url.format(page)
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1, "property_type":response.meta["property_type"], "base_url":base_url})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Agencegalerie_PySpider_france")     
        title = " ".join(response.xpath("//h1[@class='heading2 titre-detail']//text()[.!=' 0']").getall())
        if title:
            item_loader.add_value("title", re.sub("\s{2,}", " ", title)) 
    
        item_loader.add_xpath("external_id", "//div[span[.='Ref']][1]//span[@itemprop='productID']/text()")
        room_count = response.xpath("//li[span[@class='ico-chambre']]/text()[not(contains(.,'NC'))]").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split("chambre")[0])
        else:
            room_count = response.xpath("//li[span[@class='ico-piece']]/text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count.split("pièce")[0])

        address = response.xpath("//div[@class='detail-bien-ville']/text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split("(")[0].strip())
            item_loader.add_value("zipcode", address.split("(")[-1].split(")")[0].strip())
      
        item_loader.add_xpath("latitude", "//li[@class='gg-map-marker-lat']/text()")
        item_loader.add_xpath("longitude", "//li[@class='gg-map-marker-lng']/text()")
        square_meters = response.xpath("//li[span[@class='ico-surface']]/text()[not(contains(.,'NC'))]").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0])
        available_date = response.xpath("//div[contains(@class,'detail-bien-desc-content ')]/p//text()[contains(.,'Disponible le')]").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.split("Disponible le")[1].strip(), date_formats=["%d/%m/%Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        description = " ".join(response.xpath("//div[contains(@class,'detail-bien-desc-content ')]/p//text()").getall()) 
        if description:
            item_loader.add_value("description", description.strip())
            if " salle d" in description:
                bathroom_count = description.split(" salle d")[0].strip().split(" ")[-1].strip()
                if "une" in bathroom_count:
                    item_loader.add_value("bathroom_count", "1")
                elif "deux" in bathroom_count:
                    item_loader.add_value("bathroom_count", "2")
                elif "trois" in bathroom_count:
                    item_loader.add_value("bathroom_count", "3")

        images = [x for x in response.xpath("//div[@class='big-flap-container']//div[@class='diapo is-flap']/img/@data-src").getall()]
        if images:
            item_loader.add_value("images", images)
        rent =" ".join(response.xpath("//div[contains(@class,'detail-bien-prix')]/text()").getall())
        if rent:
            item_loader.add_value("rent_string", rent.strip().replace(" ",""))
        utilities = response.xpath("//li/i[span[contains(.,'charges')]]/span[@class='cout_charges_mens']/text()[.!='0']").get()
        if utilities:
            item_loader.add_value("utilities", utilities)
        deposit = response.xpath("//li[span[contains(.,'Dépôt de garantie')]]/span[@class='cout_honoraires_loc']/text()[.!='0']").get()
        if deposit:
            item_loader.add_value("deposit", deposit.replace(" ",""))

        item_loader.add_value("landlord_name", "AGENCE GALERIE")
        item_loader.add_value("landlord_phone", "04 67 66 00 60")
        item_loader.add_value("landlord_email", "location@agencegalerie.fr")
        
        yield item_loader.load_item()