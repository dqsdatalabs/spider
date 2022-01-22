# -*- coding: utf-8 -*-
import scrapy
from scrapy.http.request import Request
from ..loaders import ListingLoader


class ComarcoimmobiliItSpider(scrapy.Spider):
    name = 'comarcoimmobili_it'
    allowed_domains = ['comarcoimmobili.it']
    start_urls = ['https://www.comarcoimmobili.it/immobili.php?id_contratto=2&id_categoria=1&id_tipologia=0&riferimento=&prezzo_min=&prezzo_max=&mq_min=&mq_max=&id_provincia=0&id_comune=0']
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'development'

    def parse(self, response):
        for appartment in response.css("#contact > div:nth-child(2) > div.row>div"):
            url = "https://www.comarcoimmobili.it/" + \
                appartment.css(
                    "div.service-content.text-left>a").attrib['href']
            yield Request(url,
                          callback=self.populate_item,
                          dont_filter=True,
                          )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.css('h2.section-title::text').get().strip()

        description = response.css(
            "#top > div.about_bottom > div > div > div > div > p::text").get()

        images = response.css(
            'div.fotorama>a>img::attr(src)').extract()

        features = response.css(
            "#top>div.about_bottom>div>div>div>div>div.row>div")

        rent = None
        terrace = None
        space = None
        rooms = None
        floor = None
        other_rooms = None
        bathrooms = None
        furnished = None
        balcony = None
        parking = None
        address = None
        city = None
        for item in features:
            try:
                if "Superficie:" in item.css("span:nth-child(1)::text").get():
                    space = item.css(
                        "span:nth-child(2)::text").get().split("m")[0].strip()
                elif "Prezzo:" in item.css("span:nth-child(1)::text").get():
                    rent = item.css(
                        "span:nth-child(2)>span::text").get().split(" ")[-1].strip()
                    if "." in rent:
                        rent_array = rent.split(".")
                        rent = rent_array[0] + rent_array[1]
                    if ',' in rent:
                        rent = rent.split(',')[0]
                elif "Numero camere da letto:" in item.css("span:nth-child(1)::text").get():
                    rooms = item.css(
                        "span:nth-child(2)::text").get().strip()
                elif "Numero altre camere:" in item.css("span:nth-child(1)::text").get():
                    other_rooms = item.css(
                        "span:nth-child(2)::text").get().strip()
                elif "Numero bagni:" in item.css("span:nth-child(1)::text").get():
                    bathrooms = item.css(
                        "span:nth-child(2)::text").get().strip()
                elif "Comune:" in item.css("span:nth-child(1)::text").get():
                    city = item.css(
                        "span:nth-child(2)::text").get().strip()
                elif "Indirizzo:" in item.css("span:nth-child(1)::text").get():
                    address = item.css(
                        "span:nth-child(2)::text").get().strip()
                elif "Piano:" in item.css("span:nth-child(1)::text").get():
                    floor = item.css(
                        "span:nth-child(2)::text").get().strip()
                elif "Arredamento:" in item.css("span:nth-child(1)::text").get():
                    furnished = item.css(
                        "span:nth-child(2)::text").get().strip()
                    if "Completo" in furnished:
                        furnished = True
                    else:
                        furnished = False
                elif "Numero posti auto:" in item.css("span:nth-child(1)::text").get():
                    parking = item.css(
                        "span:nth-child(2)::text").get().strip()
                    if int(parking) >= 1:
                        parking = True
                    else:
                        parking = False
                elif "Numero di balconi:" in item.css("span:nth-child(1)::text").get():
                    balcony = item.css(
                        "span:nth-child(2)::text").get().strip()
                    if int(balcony) >= 1:
                        balcony = True
                    else:
                        balcony = False
                elif "Numero di terrazz:" in item.css("span:nth-child(1)::text").get():
                    terrace = item.css(
                        "span:nth-child(2)::text").get().strip()
                    if int(terrace) >= 1:
                        terrace = True
                    else:
                        terrace = False

            except:
                pass

        if other_rooms is None:
            other_rooms = 0
        try:
            coords = response.xpath('script[3]').get()

            coords = coords.split(".LatLng(")[1].split(");")[0]

            lat = coords.split(", ")[0]
            lng = coords.split(", ")[1]
        except:
            pass

        # # MetaData
        item_loader.add_value("external_link", response.url)
        item_loader.add_value(
            "external_id", response.url.split("_")[1].split("."))
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("title", title)
        item_loader.add_value("description", description[2:])

        # Property Details
        item_loader.add_value("property_type", "apartment")
        item_loader.add_value("square_meters", int(space))
        item_loader.add_value("room_count", int(
            rooms)+int(bathrooms)+int(other_rooms))
        item_loader.add_value("bathroom_count", bathrooms)
        item_loader.add_value("address", address)
        item_loader.add_value("floor", floor)
        item_loader.add_value("city", city)

        item_loader.add_value("latitude", lat)
        item_loader.add_value("longitude", lng)

        # Images
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))

        # Monetary Status
        item_loader.add_value("rent", int(rent))
        item_loader.add_value("currency", "EUR")

        # House Rules
        item_loader.add_value("furnished", furnished)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("parking", parking)
        item_loader.add_value("terrace", terrace)

        # LandLord Details
        item_loader.add_value("landlord_phone", "011533914")
        item_loader.add_value("landlord_email", "info@comarcoimmobili.it")
        item_loader.add_value("landlord_name", "Comarco Immobiliare")

        yield item_loader.load_item()
