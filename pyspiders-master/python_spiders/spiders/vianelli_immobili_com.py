# -*- coding: utf-8 -*-

from ..loaders import ListingLoader
from scrapy.http.request import Request
import scrapy


class VianelliImmobiliComSpider(scrapy.Spider):
    name = "vianelli_immobili_com"
    allowed_domains = ["vianelli-immobili.com"]
    start_urls = ['https://www.vianelli-immobili.com/r/annunci/affitto-.html?Codice=&Motivazione%5B%5D=2&Prezzo_da=&Prezzo_a=&Provincia=0&Totale_mq_da=&Totale_mq_a=&cf=yes&macro=1']
    country = 'italy'
    locale = 'it'
    execution_type = 'development'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)

    # 1. FOLLOWING
    def parse(self, response, **kwargs):
        appartments = response.css(
            'li.listing.realestate-lista.wow.bounceInRight')

        for appartment in appartments:
            url = appartment.css('li > section > a').attrib['href']
            yield Request(url, callback=self.populate_item)

        try:
            next_page = response.css(
                'div.paging>a.next')[-1].attrib['href']
            if "www" not in next_page:
                next_page = None
        except:
            next_page = None

        if next_page is not None:
            yield response.follow(next_page, callback=self.parse)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.css('h1.titoloscheda::text').get()

        if response.css('h1.titoloscheda > small::text').get():
            rent = response.css(
                'body > div.width_sito > h1 > small::text').get().split(" ")[-1]
        else:
            rent = title.split(" ")[-1].strip()

        if "riservata" in rent or rent is None:
            return

        if "." in rent:
            rent_array = rent.split(".")
            rent = rent_array[0] + rent_array[1]

        rent = int(rent)

        space = response.css("div.ico-24-mq>span::text").get().split(' ')[0]
        rooms = response.css(
            "div.ico-24-camere>span::text").get().split(' ')[0]

        bathrooms = response.css(
            "div.ico-24-bagni>span::text").get().split(' ')[0]

        address = response.css('h2.h2schedaimmo::text').get()
        city = address.split(",")[0]

        description = ""
        description_array = response.css('div.testo>p::text').extract()

        for item in description_array:
            description += item

        external_id = response.css('div.codice>span::text').extract()

        images = response.css(
            '#foto2 > div > div > div.swiper-wrapper > div>a::attr(href)').extract()

        appartment_type = response.css(
            "div#sezInformazioni > div:nth-child(3)::text").get()[1:]
        if appartment_type.lower() == "appartamento":
            appartment_type = "apartment"
        elif appartment_type.lower() == "mansarda" or appartment_type.lower() == "stanza - camera" or appartment_type.lower() == "attico":
            appartment_type = "room"
        elif appartment_type.lower() == "casa indipendente" or appartment_type.lower() == "cascina" or appartment_type.lower() == "villa":
            appartment_type = "house"

        properties_length = len(response.css("div#sezInformazioni > div.box"))

        elevator = None
        Floor = None
        utilities = None
        for i in range(properties_length):
            if(response.css("div#sezInformazioni > div.box > strong::text")[i].get() == "Ascensore"):
                elevator = response.css(
                    "div#sezInformazioni > div.box::text")[i].get()[1:]
                if elevator.lower() == "si":
                    elevator = True
                elif elevator.lower() == "no":
                    elevator = False
                continue
            elif(response.css("div#sezInformazioni > div.box > strong::text")[i].get() == "Piano"):
                Floor = response.css(
                    "div#sezInformazioni > div.box::text")[i].get()[1:]
                continue
            elif(response.css("div#sezInformazioni > div.box > strong::text")[i].get() == "Spese condominio"):
                utilities = response.css(
                    "div#sezInformazioni > div.box::text")[i].get().strip().split(" ")[1]
                continue

        lat = None
        long = None
        try:
            coords = response.xpath(
                '/html/body/div[4]/div[3]/div[1]/script[2]/text()').get()
            lat = coords.split('var lat = "')[1].split('";')[0]
            long = coords.split('var lgt = "')[1].split('";')[0]
        except:
            pass

        # # MetaData
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)

        # # Property Details
        item_loader.add_value("property_type", appartment_type)
        item_loader.add_value("square_meters", space)
        item_loader.add_value("room_count", rooms)
        item_loader.add_value("bathroom_count", bathrooms)
        item_loader.add_value("address", address)
        if elevator != None:
            item_loader.add_value("elevator", elevator)
        if Floor != None:
            item_loader.add_value("floor", Floor)

        item_loader.add_value("latitude", lat)
        item_loader.add_value("longitude", long)
        item_loader.add_value("city", city)

        # # Images
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))

        # # Monetary Status
        item_loader.add_value("rent", rent)
        item_loader.add_value("utilities", utilities)
        item_loader.add_value("currency", "EUR")

        # # LandLord Details
        item_loader.add_value("landlord_phone", '+39 0115628613')
        item_loader.add_value("landlord_email", "info@vianelli-immobili.com")
        item_loader.add_value("landlord_name", "Vianelli Immobili")

        yield item_loader.load_item()
