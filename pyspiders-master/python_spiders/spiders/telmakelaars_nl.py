# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector 
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re
import dateparser


class MySpider(Spider):
    name = 'telmakelaars_nl'
    start_urls = ['https://www.telmakelaars.nl/aanbod/woningaanbod/huur/aantal-40/']
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'  # LEVEL 1
    external_source = "Telmakelaars_PySpider_netherlands_nl"

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//li[contains(@class,'al2woning')]"):
            json_item = item.xpath(".//script[@type='application/ld+json']/text()").get()
            data = json.loads(json_item)

            follow_url = response.urljoin(data["url"])
            lat = data["geo"]["latitude"]
            lng = data["geo"]["longitude"]
            zipcode =  data["address"]["postalCode"]
            address =  data["address"]["streetAddress"]
            city =  data["address"]["addressLocality"]
            image = data.get("photo")
            property_type = item.xpath(".//span[contains(@class,'soortobject')]/span[@class='kenmerkValue']/text()").get()
            if "Appartement" in property_type:
                property_type = "apartment"
                yield Request(follow_url, callback=self.populate_item, meta={"lat": lat, "lng": lng,"address":address,"city":city,"zipcode":zipcode,"image":image, "property_type" : property_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Telmakelaars_PySpider_" + self.country + "_" + self.locale)
        externalid=response.url.split("huis-")[1]
        if externalid:
            external_id=externalid.split("-")[0]
            item_loader.add_value("external_id",external_id)

        lat = response.meta.get("lat")
        lng = response.meta.get("lng")
        title = response.xpath("//meta[@property='og:title']/@content").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)


        item_loader.add_css("title", "h1") 
        item_loader.add_value("external_link", response.url) 
        lat = response.meta.get("lat")
        lng = response.meta.get("lng")
        item_loader.add_value("latitude", str(lat))
        item_loader.add_value("longitude",str(lng))
        item_loader.add_value("address",response.meta.get("address"))
        item_loader.add_value("city",response.meta.get("city"))
        item_loader.add_value("zipcode",response.meta.get("zipcode"))

        item_loader.add_value("property_type", response.meta.get("property_type"))
        item_loader.add_xpath("room_count","//span[span[. ='Aantal slaapkamers']]/span[@class='kenmerkValue']/text() | //span[span[. ='Aantal kamers']]/span[@class='kenmerkValue']/ya-tr-span/text()")

        price = "".join(response.xpath("//span[span[. ='Huurprijs']]/span[@class='kenmerkValue']/text() | //span[span[. ='Huurprijs']]/span[@class='kenmerkValue']/ya-tr-span/text()").extract())
        if price:
            item_loader.add_value("rent", price.split("€")[1].split(",")[0])
            item_loader.add_value("currency", "EUR")

        utilities = "".join(response.xpath("//span[span[. ='Servicekosten']]/span[@class='kenmerkValue']/text() | //span[span[. ='Servicekosten']]/span[@class='kenmerkValue']/ya-tr-span/text()").extract())
        if utilities:
            item_loader.add_value("utilities", utilities.split("€")[1].strip())


        square = "".join(response.xpath("//span[span[. ='Woonoppervlakte']]/span[@class='kenmerkValue']/text() | //span[span[. ='Woonoppervlakte']]/span[@class='kenmerkValue']//ya-tr-span/text()").extract())
        if square:
            item_loader.add_value("square_meters", square.split("m²")[0])

        floor = "".join(response.xpath("//span[span[. ='Aantal woonlagen']]/span[@class='kenmerkValue']/text() | //span[span[. ='Aantal woonlagen']]/span[@class='kenmerkValue']/ya-tr-span/text()").extract())
        if floor:
            item_loader.add_value("floor", floor.strip().replace("woonlaag",""))

        images = [response.urljoin(x)for x in response.xpath("//div[@class='ogFotos']//div/span[@class='fotolist ']/a/@href").extract()]
        if images:
                item_loader.add_value("images", images)

        try:
            available_date = response.xpath("normalize-space(//span[span[. ='Aanvaarding']]/span[@class='kenmerkValue']/text())").extract_first()
            if available_date:
                if "In overleg" not in available_date or "Direct" not in available_date:
                    date_parsed = dateparser.parse(available_date, date_formats=["%d %B %Y"])
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
        except:
            pass

        desc = "".join(response.xpath("//div[@id='Omschrijving']/text()").extract())
        desc = re.sub('\s{2,}', ' ', desc)
        item_loader.add_value("description", desc)


        terrace = "".join(response.xpath("//span[span[. ='Balkon']]/span[@class='kenmerkValue']/text() | //span[span[. ='Balkon']]/span[@class='kenmerkValue']/ya-tr-span/text()").extract())
        if terrace:
            if "Ja" in terrace:
                item_loader.add_value("balcony", True)


        terrace = "".join(response.xpath("//span[span[. ='Parkeerfaciliteiten']]/span[@class='kenmerkValue']/text() | //span[span[. ='Parkeerfaciliteiten']]/span[@class='kenmerkValue']/ya-tr-span/text()").extract())
        if terrace:
                item_loader.add_value("parking", True)

        terrace = "".join(response.xpath("//span[span[. ='Voorzieningen']]/span[@class='kenmerkValue']/text()[contains(.,'Lift')] | //span[span[. ='Voorzieningen']]/span[@class='kenmerkValue']/ya-tr-span/text()[contains(.,'Lift')]  ").extract()).strip()
        if terrace:
            item_loader.add_value("elevator", True)

        terrace = "".join(response.xpath("//span[span[. ='Bijzonderheden']]/span[@class='kenmerkValue']/text()[contains(.,'Gestoffeerd')]| //span[span[. ='Bijzonderheden']]/span[@class='kenmerkValue']/ya-tr-span/text()[contains(.,'Gestoffeerd')]").extract()).strip()
        if terrace:
            item_loader.add_value("furnished", True)

        item_loader.add_value("landlord_phone", "020-3012949")
        item_loader.add_value("landlord_email", "info@telmakelaars.n")
        item_loader.add_value("landlord_name", "Tel Makelaars")

        status=response.xpath("//span/span[contains(.,'Status')]/following-sibling::span/text()[not(contains(.,'Verhuurd'))]").get()
        if status:
            yield item_loader.load_item()