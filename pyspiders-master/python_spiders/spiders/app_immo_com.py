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
    name = 'app_immo_com'
    execution_type='testing'
    country='france'
    locale='fr'

    def start_requests(self):
        start_urls = [
            {
                "url" : "http://www.app-immo.com/recherche,basic.htm?idqfix=1&idtt=1&idtypebien=1&tri=d_dt_crea&cp=,&",
                "property_type" : "apartment",
            },

        ]
        for item in start_urls:
            yield Request(item["url"],
                        callback=self.parse,
                        meta={"property_type": item["property_type"]})

    # 1. FOLLOWING
    def parse(self, response):
      
        for item in response.xpath("//div[@class='span9']/a"):
            url = response.urljoin(item.xpath("./@href").extract_first())
            yield Request(url,callback=self.populate_item , meta={"property_type": response.meta.get('property_type')})

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type",response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "App_Immo_PySpider_france")
        item_loader.add_xpath("title", "//title/text()")
        item_loader.add_xpath("external_id", "substring-after(//div[@class='bloc-detail-reference']/span/text(),': ')")

        item_loader.add_xpath("address", "//h1[@itemprop='name']/text()[2]")
        item_loader.add_xpath("city", "substring-before(//h1[@itemprop='name']/text()[2],'(')")
        item_loader.add_xpath("zipcode", "substring-before(substring-after(//h1[@itemprop='name']/text()[2],'('),')')")
        item_loader.add_xpath("floor", "normalize-space(//li[@title='Etage']/div[2]/text())")

        item_loader.add_xpath("bathroom_count", "normalize-space(//li[(contains(@title,'Salle'))]/div[2]/text())")

        rent = response.xpath("normalize-space(//div[contains(@class,'h1-like')]/span[@itemprop='price']/text())").extract_first()
        if rent:
            item_loader.add_value("rent", rent.replace("\xa0","").replace(" ","").strip())
        item_loader.add_value("currency", "EUR")

        energy_label = response.xpath("normalize-space(//div[@class='row-fluid']/div[contains(@class,'dpe-bloc-lettre')]/text())").extract_first()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.strip())

        room_count = response.xpath("normalize-space(//li[contains(@title,'Chambre')]/div[2]/text())").extract_first()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        else:     
            room_count = "".join(response.xpath("//li[(contains(@title,'Pièce'))]/div[2]/text()").extract())
            if room_count:
                item_loader.add_value("room_count", room_count.strip())


        deposit = response.xpath("substring-after(//div[@class='row-fluid']/strong/text(),': ')").extract_first()
        if deposit:
            item_loader.add_value("deposit", deposit.replace("\xa0","").replace(" ","").strip())

        utilities = response.xpath("//div[@class='hidden-phone']/ul/li[contains(.,'Provisions pour charge')]/text()").extract_first()
        if utilities:
            utilities = utilities.split(":")[1].split("€")[0].strip()
            item_loader.add_value("utilities", utilities.replace("\xa0","").replace(" ","").strip())

        lat_lng = response.xpath("substring-before(substring-after(//script[@type='text/javascript']//text(),'MAP_CENTER_LATITUDE: '),',')").extract_first()
        if lat_lng:
            item_loader.add_value("latitude", lat_lng.replace('"','').strip())
            lon = response.xpath("substring-before(substring-after(//script[@type='text/javascript']//text(),'MAP_CENTER_LONGITUDE: '),',')").extract_first()
            item_loader.add_value("longitude", lon.replace('"','').strip())

        description = " ".join(response.xpath("//p[@itemprop='description']/text()").getall())   
        if description:
            item_loader.add_value("description", description.strip())

        images = [response.urljoin(x) for x in response.xpath("//div[@class='nivoSlider z100']/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)

        available_date=response.xpath("substring-after(//p[@class='dt-dispo']/text(),': ')").get()
        if available_date:
            date2 =  available_date.strip()
            date_parsed = dateparser.parse(
                date2, date_formats=["%m-%d-%Y"]
            )
            date3 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date3)

        surface = " ".join(response.xpath("//li[@title='Surface']/div[2]/text()").getall())   
        if surface:
            s_meters = surface.split("m²")[0].replace(",",".").strip()
            item_loader.add_value("square_meters", int(float(s_meters)))



        elevator = response.xpath("normalize-space(//li[@title='Ascenseur']/div[2]/text())").extract_first()
        if elevator:
            if "oui" in elevator.lower():
                item_loader.add_value("elevator", True)
            else:
                item_loader.add_value("elevator", False)

        balcony = "".join(response.xpath("//li[contains(@title,'Balcon')]/div[2]/text()").extract())
        if balcony:
            item_loader.add_value("balcony", True)

        parking = "".join(response.xpath("//li[contains(@title,'Parking')]/div[2]/text()").extract())
        if parking:
            item_loader.add_value("parking", True)

        item_loader.add_value("landlord_name", "APP IMMOBILIER")
        item_loader.add_value("landlord_phone", "01 69 44 22 22")
        item_loader.add_value("landlord_email", "accueil@app-immo.com")
        

    

        yield item_loader.load_item()


