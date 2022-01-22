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
    name = 'vielmon_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://www.vielmon.fr/recherche,basic.htm?idqfix=1&idtt=1&idtypebien=1&saisie=O%c3%b9+d%c3%a9sirez-vous+habiter+%3f&tri=d_dt_crea&",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "http://www.vielmon.fr/recherche,basic.htm?idqfix=1&idtt=1&idtypebien=2&saisie=O%c3%b9+d%c3%a9sirez-vous+habiter+%3f&tri=d_dt_crea&",
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
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[@class='row-fluid']/div/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta['property_type']})
            seen = True
        
        if response.meta["property_type"] == "apartment" and (page == 2 or seen):
            p_url = f"http://www.vielmon.fr/recherche,incl_recherche_listing_ajax.htm?surfacemin=Min&surfacemax=Max&surf_terrainmin=Min&surf_terrainmax=Max&px_loyermin=Min&px_loyermax=Max&idqfix=1&idtt=1&pres=listing&idtypebien=1&lang=fr&ANNLISTEpg={page}&tri=d_dt_crea&_=1609762801193"
            yield Request(
                p_url,
                callback=self.parse,
                meta={'property_type': response.meta['property_type'], "page":page+1}
            )
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Homeinvestbelgium_PySpider_france")
        item_loader.add_xpath("title", "//title/text()")

        external_id = "".join(response.xpath("//div[@class='bloc-detail-reference']/span[1]/text()[contains(.,'Référence')]").extract())
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[1].strip())

        rent = "".join(response.xpath("normalize-space(//span[@itemprop='price']/text())").extract())
        if rent:
            item_loader.add_value("rent", rent.strip().replace("\xa0",""))
        item_loader.add_value("currency", "EUR")

        meters = "".join(response.xpath("//div[@class='span12']//li[@title='Surface']/div[2]/text()").extract())
        if meters:
            s_meters = meters.strip().split("m²")[0].replace(",",".").strip()
            item_loader.add_value("square_meters", int(float(s_meters)))

        room = "".join(response.xpath("//div[@class='span12']//li[contains(@title,'Chambre')]/div[2]/text()").extract())
        if room:
            item_loader.add_value("room_count", room.strip())
        else:
            room = "".join(response.xpath("//div[@class='span12']//li[@title='Pièce']/div[2]/text()").extract())
            if room:
                item_loader.add_value("room_count", room.strip())

  
        bathroom_count = "".join(response.xpath("//div[@class='span12']//li[@title='Salle de bain']/div[2]/text()").extract())
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        Latlng = "".join(response.xpath("//script/text()[contains(.,'LATITUDE')]").extract())
        if bathroom_count:
            lat = "".join(response.xpath("substring-after(substring-after(//script/text()[contains(.,'LATITUDE')],'ANNONCE: {'),'LATITUDE: ')").extract())
            lng = "".join(response.xpath("substring-after(substring-after(//script/text()[contains(.,'LATITUDE')],'ANNONCE: {'),'LONGITUDE: ')").extract())
            item_loader.add_value("latitude", lat.split('"')[1].split('"')[0])
            item_loader.add_value("longitude", lng.split('"')[1].split('"')[0])

        floor = "".join(response.xpath("//div[@class='span12']//li[@title='Etage']/div[2]/text()").extract())
        if floor:
            item_loader.add_value("floor", floor.strip())

        address = "".join(response.xpath("//h1[@itemprop='name']/text()[2]").extract())
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.split("(")[0].strip())
            item_loader.add_value("zipcode", address.split("(")[1].split(")")[0].strip())


        deposit = "".join(response.xpath("//div[@class='row-fluid']/strong[contains(.,'Dépôt de garantie')]/text()[not(contains(.,'N/A'))]").extract())
        if deposit:
            dep =  deposit.split(":")[1].split("€")[0].strip()
            item_loader.add_value("deposit", int(float(dep)))

        utilities = "".join(response.xpath("//div[@class='row-fluid']//strong[contains(.,'Honoraires')]/text()[not(contains(.,'N/A'))]").extract())
        if utilities:
            uti =  utilities.split(":")[1].split("€")[0].strip()
            item_loader.add_value("utilities", int(float(uti)))

        energy_label = "".join(response.xpath("normalize-space(//div[contains(@class,'span2')]/text())").extract())
        if energy_label:
            item_loader.add_value("energy_label", energy_label.upper())

        desc = "".join(response.xpath("//p[@itemprop='description']/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

        images = [x for x in response.xpath("//div[@id='slider']/a/@href").extract()]
        if images is not None:
            item_loader.add_value("images", images) 

        available_date="".join(response.xpath("//div[@class='bloc-detail-reference']/text()").getall())
        if available_date:
            date2 =  available_date.split(":")[1].strip()
            date_parsed = dateparser.parse(
                date2, date_formats=["%m-%d-%Y"]
            )
            date3 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date3)

        balcony = "".join(response.xpath("//div[@class='span12']//li[@title='Balcon']/div[2]/text()").extract())
        if balcony:
            if "0" not in balcony:
                item_loader.add_value("balcony", True)
            elif "0" in balcony:
                item_loader.add_value("balcony", False)

        furnished = "".join(response.xpath("//div[@class='span12']//li[@title='Meublé']/div[2]/text()").extract())
        if furnished:
            if "oui" in furnished:
                item_loader.add_value("furnished", True)

        elevator = "".join(response.xpath("//div[@class='span12']//li[@title='Ascenseur']/div[2]/text()").extract())
        if elevator:
            if "oui" in elevator:
                item_loader.add_value("elevator", True)

        parking = "".join(response.xpath("//div[@class='span12']//li[@title='Parking']/div[2]/text()").extract())
        if parking:
            if "0" not in parking:
                item_loader.add_value("parking", True)
            elif "0" in parking:
                item_loader.add_value("parking", False)

        item_loader.add_value("landlord_phone", "01 30 61 23 23")
        item_loader.add_value("landlord_name", "VIELMON IMMOBILIER")  
      
        yield item_loader.load_item()