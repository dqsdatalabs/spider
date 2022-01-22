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
    name = 'pole_immobilier_caen_com'
    execution_type='testing'
    country='france'
    locale='fr'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://www.pole-immobilier-caen.com/recherche,incl_recherche_basic_ajax.htm?surfacemin=Min&surfacemax=Max&surf_terrainmin=Min&surf_terrainmax=Max&px_loyermin=Min&px_loyermax=Max&idqfix=1&idtt=1&pres=basic&lang=fr&idtypebien=1&ANNLISTEpg={}&_=1613571110328",
                ],
                "property_type" : "apartment",
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
        seen = False

        for item in response.xpath("//div[@id='recherche-resultats-listing']/div/div/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True

        if page == 2 or seen:
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
        item_loader.add_value("external_link", response.url.split("?")[0])
        item_loader.add_value("external_source", "Pole_Immobilier_Caen_PySpider_france") 
        item_loader.add_xpath("title", "//title/text()") 
        item_loader.add_xpath("external_id", "substring-after(//div[@class='bloc-detail-reference']/span/text(),': ')") 

        address = "".join(response.xpath("//h1[@itemprop='name']/text()[2]").extract())
        if address:
            city = address.split("(")[0].strip()
            zipcode = address.split("(")[1].split(")")[0].strip()
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
            item_loader.add_value("address", "{} {}".format("".join(item_loader.get_collected_values("zipcode")),"".join(item_loader.get_collected_values("city"))))


        rent = "".join(response.xpath("normalize-space(//div[contains(@class,'h1-like')]/span[@itemprop='price']/text())").extract())
        if rent:
            price = rent.replace("\xa0","").replace(" ","").strip()
            item_loader.add_value("rent", price.strip())
            item_loader.add_value("currency", "EUR")

        available_date=response.xpath("substring-after(//div[@class='bloc-detail-reference']/text(),': ')").get()
        if available_date:
            date2 =  available_date.strip()
            date_parsed = dateparser.parse(
                date2, date_formats=["%m-%d-%Y"]
            )
            date3 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date3)

        description = " ".join(response.xpath("//div[@class='description']/p//text()").getall())  
        if description:
            item_loader.add_value("description", description.strip())

        utilities = "".join(response.xpath("//div[@class='hidden-phone']/ul/li/text()").extract())
        if utilities:
            uti = utilities.split(":")[1].strip().replace("\xa0","")
            item_loader.add_value("utilities", uti.strip())

        deposit = "".join(response.xpath("//div[@class='row-fluid']/strong/text()").extract())
        if deposit:
            deposit = deposit.split(":")[1].strip().replace("\xa0","").replace(" ","")
            item_loader.add_value("deposit", deposit.strip())

        item_loader.add_xpath("energy_label", "normalize-space(//div[@class='row-fluid']/div[contains(@class,'dpe-bloc-lettre')]/text())")

        images = [x for x in response.xpath("//div[@class='nivoSlider z100']/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)

        meters = " ".join(response.xpath("//li[div[.='Surface']]/div[2]/text()").getall())  
        if meters:
            s_meters = meters.split("m²")[0].replace(",",".").replace("\xa0","").replace(" ","").strip()
            item_loader.add_value("square_meters", int(float(s_meters))) 

        floor = " ".join(response.xpath("//li[div[contains(.,'Etage')]]/div[2]/text()").getall())  
        if floor:
            item_loader.add_value("floor", floor.strip()) 

        room = "".join(response.xpath("//li[div[contains(.,'Chambre')]]/div[2]/text()").extract())
        if room:     
            item_loader.add_value("room_count", room.strip())
        else:
        
            room = "".join(response.xpath("//li[div[contains(.,'Pièce')]]/div[2]/text()").extract())
            if room:
                item_loader.add_value("room_count", room.strip())

        bathroom_count = "".join(response.xpath("//li[div[contains(.,'Salle d')]]/div[2]/text()").extract())
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        furnished = "".join(response.xpath("//li[div[contains(.,'Meublé')]]/div[2]/text()").getall())
        if furnished:
            if "oui" in furnished.lower():
                item_loader.add_value("furnished",True)
            elif "non" in furnished.lower():
                item_loader.add_value("furnished",False)

        balcony = "".join(response.xpath("//li[div[contains(.,'Balcon')]]/div[2]/text()").getall())
        if balcony:
            if balcony !="0":
                item_loader.add_value("balcony",True)
            elif balcony == "0":
                item_loader.add_value("balcony",False)

        item_loader.add_value("landlord_phone", "+33231729089")
        item_loader.add_value("landlord_name", "POLE IMMOBILIER ")


        yield item_loader.load_item()