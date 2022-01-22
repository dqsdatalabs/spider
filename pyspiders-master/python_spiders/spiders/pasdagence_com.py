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
    name = 'pasdagence_com'
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    start_urls = ["http://www.pasdagence.com/public/rechercloc.php"]

    def parse(self, response):

        depart_array = response.xpath("//select[@name='depart']/option[contains(.,'[')]/@value").getall()

        start_urls = [
            {
                "value" : "1",
                "property_type" : "apartment"
            },
            {
                "value" : "2",
                "property_type" : "house"
            },   
        ]
        for url in start_urls:
            p_value = url.get("value")
            for dep in depart_array:
                f_url = f"http://www.pasdagence.com/public/location-immobilier-particulier.php?depart={dep}&vill=800&iddepart=&typebien={p_value}&tranchpiece=5&tranchprix=13&venteachat=2&image.x=174&image.y=36"
                yield Request(f_url,
                            callback=self.jump,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def jump(self, response):
        for item in response.xpath("//div[@id='annonceventephotoph']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        
        next_page = response.xpath("//img[contains(@src,'suivante_05')]/../@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.jump, meta={'property_type': response.meta.get('property_type')})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source","Pasdagence_PySpider_france")

        load_time = response.xpath("//h1//text()").get()
        if load_time:
            import dateparser
            date_parsed = dateparser.parse(load_time.split(':')[-1].strip(), date_formats=["%d/%m/%Y"], languages=['fr'])
            if date_parsed and date_parsed.year < 2020: return

        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title)

        rent = "".join(response.xpath("//div[span[.='Prix :']]/span[2]/text()[.!=' 0 €']").getall())
        if rent:
            price = rent.strip().replace(" ","")
            item_loader.add_value("rent_string", price)
        else:            
            rent = response.xpath("substring-after(//div[@id='txtdescript']//div/span[contains(.,'loué') and contains(.,'euro')],'loué')").get()
            if rent:
                price = rent.strip().replace(" ","")
                item_loader.add_value("rent_string", price)
            else:
                rent = response.xpath("substring-after(//div[@id='txtdescript']//div/span[contains(.,'loyer') and contains(.,'€')],'loyer')").get()
                if rent:
                    price = rent.strip().replace(" ","")
                    item_loader.add_value("rent_string", price)

        item_loader.add_value("currency", "EUR")

        room_count = "".join(response.xpath("//tr/td[span[.='Nombre de chambres: ']]/span[2]/text()").getall())
        if room_count:
            item_loader.add_value("room_count", room_count.strip())

        bathroom_count = "".join(response.xpath("//tr/td[span[.='Nombre de salles de bain: ']]/span[2]/text()").getall())
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        meters = "".join(response.xpath("//tr/td[span[.='Surface habitable: ']]/span[2]/text()").getall())
        if meters:
            item_loader.add_value("square_meters", meters.split("m²")[0].strip())

        address = "".join(response.xpath("concat(//td/span[@class='txttitreannonce']/strong/text(), ' ',//td/span[@class='txttitre2annonce']/text())").getall())
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_xpath("city","//td/span[@class='txttitre2annonce']/text()")

        description = " ".join(response.xpath("//span[@class='Style6']/text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))

        images = [ response.urljoin(x) for x in response.xpath("//a[@class='NomJeuxHome']/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        available_date="".join(response.xpath("substring-after(//h1/span/text(),':')").getall())

        if available_date:
            date2 =  available_date.strip()
            date_parsed = dateparser.parse(
                date2, date_formats=["%m-%d-%Y"]
            )
            date3 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date3)

        terrain = "".join(response.xpath("//tr/td[span[.='Surface du terrain: ']]/span[2]/text()").getall())
        if terrain:
            item_loader.add_value("terrace", True)

        item_loader.add_value("landlord_name", "Pasdagence")

       
        yield item_loader.load_item()
