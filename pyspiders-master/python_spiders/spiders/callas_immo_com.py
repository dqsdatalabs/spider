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
    name = 'callas_immo_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://www.callas-immo.com/recherche?a=2&b%5B%5D=appt&c=&f=&e=&do_search=Rechercher",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "http://www.callas-immo.com/recherche?a=2&b%5B%5D=house&c=&radius=0&d=1&e=illimit%C3%A9&f=0&x=illimit%C3%A9&do_search=Rechercher",
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[@class='res_tbl1']/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Callas_Immo_PySpider_france")
        
        title = response.xpath("//h1/text()").get()
        item_loader.add_value("title", title)
        
        external_id = response.xpath("//tr/td[contains(.,'Référence')]/following-sibling::td/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)
        
        rent = response.xpath("//td/span[contains(.,'€')]/text()").get()
        if rent:
            rent = rent.split("€")[0].replace(" ","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")
        
        deposit = response.xpath("//div[@class='basic_copro']//text()[contains(.,'Dépôt ')]").get()
        if deposit:
            deposit = deposit.split("Dépôt de garantie")[1].split("€")[0].replace(" ","")
            item_loader.add_value("deposit", deposit)
        
        utilities = response.xpath("//tr/td[contains(.,'Charges')]/following-sibling::td//text()").get()
        if utilities:
            utilities = utilities.split("€")[0].strip()
            item_loader.add_value("utilities", utilities)
        
        address = "".join(response.xpath("//tr/td[contains(.,'Ville')]/following-sibling::td//text()").getall())
        if address:
            item_loader.add_value("address", address.strip())
        
        city = response.xpath("//tr/td[contains(.,'Ville')]/following-sibling::td//text()").getall()
        if city:
            item_loader.add_value("city", city[0])
            item_loader.add_value("zipcode", city[1])
            
        square_meters = response.xpath("//tr/td[contains(.,'Surface')]/following-sibling::td//text()").get()
        if square_meters:
            square_meters = square_meters.strip().replace(",",".")
            item_loader.add_value("square_meters", int(float(square_meters)))
        
        room_count = response.xpath("//tr/td[contains(.,'Chambres')]/following-sibling::td//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        else:
            room_count = response.xpath("//tr/td[contains(.,'Pièces')]/following-sibling::td//text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count = response.xpath("//tr/td[contains(.,'Salle')]/following-sibling::td//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        furnished = response.xpath("//tr/td[contains(.,'Ameublement')]/following-sibling::td//text()[not(contains(.,'Non'))]").get()
        if furnished:
            if "oui" in furnished.lower():
                item_loader.add_value("furnished", True)
        
        elevator = response.xpath("//tr/td[contains(.,'Ascenseur')]/following-sibling::td//text()[not(contains(.,'Non'))]").get()
        if elevator:
            if "oui" in elevator.lower():
                item_loader.add_value("elevator", True)
                
        swimming_pool = response.xpath("//tr/td[contains(.,'Piscine')]/following-sibling::td//text()[not(contains(.,'Non'))]").get()
        if swimming_pool:
            if "oui" in swimming_pool.lower():
                item_loader.add_value("swimming_pool", True)
        
        balcony = response.xpath("//tr/td[contains(.,'Balcon')]/following-sibling::td//text()[not(contains(.,'Non'))]").get()
        if balcony:
            if "oui" in balcony.lower():
                item_loader.add_value("balcony", True)
        
        floor = response.xpath("//tr/td[contains(.,'Étage')]/following-sibling::td//text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())
                
        description = " ".join(response.xpath("//div[@itemprop='description']/text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        
        import dateparser
        available_date = response.xpath("//tr/td[contains(.,'Disponibilité')]/following-sibling::td//text()[not(contains(.,'Immédiate'))]").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.replace("début","").strip(), date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        images = [x for x in response.xpath("//a[@class='rsImg']//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        item_loader.add_value("landlord_name", "Callas Immo")
        item_loader.add_value("landlord_phone", "09 50 56 76 70")
        item_loader.add_value("landlord_email", "contact@callas-immo.com")
        
        yield item_loader.load_item()