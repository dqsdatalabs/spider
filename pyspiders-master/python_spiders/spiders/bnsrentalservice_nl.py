# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request
from python_spiders.loaders import ListingItem
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser


class MySpider(Spider):
    name = "bnsrentalservice_nl" # same with expatrealestate_nl & estata_nl
    start_urls = [
        "https://www.bnsrentalservice.nl/nl/realtime-listings/consumer"
    ]  # LEVEL 1
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'

    # 1. FOLLOWING LEVEL 1
    def parse(self, response):
        
        jresp = json.loads(response.body)
        for item in jresp:
            item_loader = ListingLoader(response=response)
            if item["isRentals"]:
                follow_url = response.urljoin(item.get('url'))

                property_type = item.get('mainType')
                if property_type not in ["apartment","house","student_apartment","room","studio"]:
                    propperty_type = None
                item_loader.add_value("room_count", str(item.get('rooms')))
                item_loader.add_value("address", item.get('address'))
                item_loader.add_value("city", item.get('city'))
                item_loader.add_value("zipcode", item.get('zipcode'))
                item_loader.add_value("latitude", str(item.get('lat')))
                item_loader.add_value("longitude", str(item.get('lng')))
                item_loader.add_value("external_link", follow_url)


                yield response.follow(follow_url, self.populate_item, meta={'item': item_loader, "property_type":property_type})

    # 2. SCRAPING LEVEL 2
    def populate_item(self, response):

        item_loader = response.meta.get('item')

        property_type = response.meta.get('property_type')
        if property_type:
            item_loader.add_value("property_type", property_type)
        else:
            return

        item_loader.add_value("external_source", "Bnsrentalservice_PySpider_" + self.country + "_" + self.locale)
        
        available_date ="".join(response.xpath("//dl[@class='full-details']/dt[.='Oplevering']/following-sibling::dd[1]/text()[.!='Per direct']").extract())
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d %B %Y"])
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)
        
        title="".join(response.xpath("//title/text()").getall()).strip()
        title=title.split("Face")[0]
        if title:
            item_loader.add_value("title", title.strip())
            
        item_loader.add_value("external_link", response.url)

        price = response.xpath("//dl[@class='full-details']/dt[.='Prijs']/following-sibling::dd[1]/text()[contains(.,'€')]").extract_first()
        if price:
            item_loader.add_value("rent", price.split("€")[1].strip().split("p")[0])
            item_loader.add_value("currency", "EUR")
        balcony = response.xpath("//dl[@class='full-details']/dt[contains(.,'Balkon')]/following-sibling::dd[1]/text()").extract_first()
        if balcony:
            if balcony == "Ja":
                item_loader.add_value("balcony", True) 

        square = response.xpath("normalize-space(//dl[@class='full-details']/dt[.='Woonoppervlakte']/following-sibling::dd[1]/text())").get()
        if square:
            item_loader.add_value("square_meters", square.split("m²")[0])

        images = [response.urljoin(x)for x in response.xpath("//div[@class='responsive-gallery-item']/div/img/@data-src").extract()]
        if images:
                item_loader.add_value("images", images)

        desc = "".join(response.xpath("//p[@class='object-description']/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
        
        if "voorschot" in desc:
            utilities = desc.split("voorschot ")[1].split("pm")[0].split("€")[1].strip()
            item_loader.add_value("utilities", utilities.split(",")[0])

        if "wasmachine" in desc:
            item_loader.add_value("washing_machine", True)
            
        if "Geen huisdieren toegestaan" in desc:
            item_loader.add_value("pets_allowed", False)
        elif "Ja huisdieren toegestaan" in desc:
            item_loader.add_value("pets_allowed", True)

        if "etage" in desc.lower():
            floor=desc.split(" etage")[0].strip().split(" ")[-1]
            floor=floor_trans(floor)
            if floor:
                item_loader.add_value("floor",floor.strip())

        terrace = response.xpath("//dl[@class='full-details']/dt[.='Inrichting']/following-sibling::dd[1]/text()").get()
        if terrace:
            item_loader.add_value("furnished", True)
        bathroom = response.xpath("//dl[@class='full-details']/dt[contains(.,'Aantal badkamers')]/following-sibling::dd/text()").extract_first()
        
        if bathroom:
            item_loader.add_xpath("bathroom_count",bathroom)
        phone = "".join(response.xpath("normalize-space(//p[@class='footerAdres']/text()[contains(.,'Tel:')])").extract())
        if phone:
            item_loader.add_value("landlord_phone", phone.replace("Tel:","").strip())
        item_loader.add_xpath("landlord_phone", "normalize-space(//p[@class='footerAdres']/a/text())")
        item_loader.add_value("landlord_email", "info@RentalRotterdam.nl")
        item_loader.add_value("landlord_name", "B&S Rental Service")

        yield item_loader.load_item()

def floor_trans(floor):
    
    if floor.replace("e","").replace("ste","").isdigit():
        return floor.replace("e","")
    elif "eerste" in floor.lower():
        return "1"
    elif "tweede" in floor.lower():
        return "2"
    elif "derde" in floor.lower():
        return "3"
    elif "vierde" in floor.lower():
        return "4"
    elif "vijfde" in floor.lower():
        return "5"
    elif "achtste" in floor.lower():
        return "8"
    elif "bovenste" in floor.lower() or "hoogste" in floor.lower():
        return "upper"
    else :
        return False