from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re



class MySpider(Spider):
    name = 'andreimmo_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = "Andreimmofr_PySpider_france"

    custom_settings = {
       "HTTPCACHE_ENABLED": False,
    }

    def start_requests(self):

        yield Request("https://andreimmo.fr/vous-etes-locataire/", callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):
        for url in response.xpath("//a[@class='es-property-link']/@href").getall():
            
            yield Request(url, callback=self.populate_item)

        next_btn = response.xpath("//a[@class='next page-numbers']/@href").get()
        if next_btn:
            yield Request(next_btn, callback=self.parse)

            

        

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        title = response.xpath("//h1[@class='entry-title']/text()").get()
        if title:
            item_loader.add_value("title",title)
            zipcode = re.search("[\d]+",title)[0]
            item_loader.add_value("zipcode",zipcode)

        rent = response.xpath("//span[@class='es-price']/span").get()
        if rent:
            rent = rent.split(",")[0].replace(".","")
            item_loader.add_value("rent",rent)

        address = response.xpath("//div[@class='es-address']/text()").get()
        if address:
            item_loader.add_value("address",address)

        bathroom_count = response.xpath("//strong[contains(text(),'bains')]/following-sibling::text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.strip())

        room_count = response.xpath("//strong[contains(text(),'pièces')]/following-sibling::text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.strip())

        floor = response.xpath("//strong[contains(text(),'Etages')]/following-sibling::text()").get()
        if floor:
            item_loader.add_value("floor",floor.strip())  

        square_meters = response.xpath("//strong[contains(text(),'Superficie')]/following-sibling::text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.replace("m²","").split(".")[0].strip())  


        city = response.xpath("//strong[contains(text(),'Ville')]/following-sibling::text()").get()
        if city:
            item_loader.add_value("city",city.strip())

        property_type = response.xpath("//strong[contains(text(),'Type')]/following-sibling::a/text()").get()
        if property_type:
            if "appartement" in property_type.lower():
                item_loader.add_value("property_type","apartment")
            if "studio" in property_type.lower():
                item_loader.add_value("property_type","studio")


        desc = " ".join(response.xpath("//div[@id='es-description']/h3/text()").getall())
        if desc:
            item_loader.add_value("description",desc)

        deposit = re.search("Garantie : ([\d]+)", desc)
        if deposit:
            deposit=deposit.group(1)
            item_loader.add_value("deposit",deposit)

        energy_point = response.xpath("//span[@class='diagnostic-number']/text()").get()
        if energy_point:
            energy_label = energy_label_calculate(energy_point)
            item_loader.add_value("energy_label",energy_label)
        utilities=response.xpath("//h3//text()[contains(.,'Provision sur charges ')]").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split(":")[-1].split("€")[0].strip())

        images = " ".join(response.xpath("//img[@class='attachment-thumbnail size-thumbnail']//@src").getall())
        if images:
            item_loader.add_value("images",images)
            item_loader.add_value("external_images_count",len(images))

        external_id = response.xpath("//link[@rel='shortlink']/@href").get()
        if external_id:
            external_id = external_id.split("p=")[0]
            item_loader.add_value("external_id",external_id)

        item_loader.add_value("landlord_name","Andre real estate")
        item_loader.add_value("landlord_email","agence@andreimmo.fr")
        item_loader.add_value("landlord_phone","01.71.52.75.02")
        item_loader.add_value("currency","EUR")

        yield item_loader.load_item()


def energy_label_calculate(energy_number):
    energy_number = int(float(energy_number))
    energy_label = ""
    if energy_number <= 50:
        energy_label = "A"
    elif energy_number > 50 and energy_number <= 90:
        energy_label = "B"
    elif energy_number > 90 and energy_number <= 150:
        energy_label = "C"
    elif energy_number > 150 and energy_number <= 230:
        energy_label = "D"
    elif energy_number > 230 and energy_number <= 330:
        energy_label = "E"
    elif energy_number > 330 and energy_number <= 450:
        energy_label = "F"
    elif energy_number > 450:
        energy_label = "G"
    return energy_label