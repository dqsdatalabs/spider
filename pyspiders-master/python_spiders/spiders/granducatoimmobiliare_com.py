# -*- coding: utf-8 -*-
import scrapy
from scrapy.http.request import Request
from ..loaders import ListingLoader


class GranducatoimmobiliareComSpider(scrapy.Spider):
    name = 'granducatoimmobiliare_com'
    allowed_domains = ['granducatoimmobiliare.com']
    start_urls = ['https://www.granducatoimmobiliare.com/annunci-immobiliari/all-tipologia/all-zona/Affitto/Residenziale/all-citta/all-prezzo-minimo-euro/all-prezzo-massimo-euro/all-riferimento-annuncio/dataannuncio-0.html']
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'development'

    def parse(self, response):
        for appartment in response.css("div.list-properties.right-space>div"):
            url = "https://www.granducatoimmobiliare.com/" + \
                appartment.css(
                    "div.property-images>a").attrib['href']
            yield Request(url,
                          callback=self.populate_item,
                          dont_filter=True
                          )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.css(
            '#content > div > div > div.span9 > div > h2::text').get().strip()

        description = response.css(
            "#content > div > div > div.span9 > div > div > p:nth-child(5)::text").get()

        images = response.css(
            'div.es-carousel>ul>li>a>img::attr(src)').extract()

        features = response.css(
            "div.detail-properties.right-space>table>tbody>tr>td")

        city = response.css(
            '#content > div > div > div.span9 > div > div > p:nth-child(4) > span:nth-child(1)::text').get().split(": ")[1]

        rent = None
        space = None
        rooms = None
        floor = None
        bathrooms = None
        address = None
        external_id = None
        furnished = None
        balcony = None
        terrace = None
        parking = None
        elevator = None
        date = None
        utils = None
        for item in features:
            try:
                if "Bagni:" in item.css("strong::text").get():
                    bathrooms = item.css("td::text").get().strip()
                elif "Piano:" in item.css("strong::text").get():
                    floor = item.css("td::text").get().strip()
                elif "Data annuncio:" in item.css("strong::text").get():
                    date = item.css("td::text").get().strip()
                elif "Zona:" in item.css("strong::text").get():
                    address = item.css("td::text").get().strip()
                elif "Riferimento:" in item.css("strong::text").get():
                    external_id = item.css("td::text").get().strip()
                elif "Superficie:" in item.css("strong::text").get():
                    space = item.css("td::text").get().strip().split(" ")[0]
                elif "Arredamento:" in item.css("strong::text").get():
                    furnished = item.css("td::text").get().strip()
                    if "ARREDATO" in furnished:
                        furnished = True
                    else:
                        furnished = False
                elif "Prezzo:" in item.css("strong::text").get():
                    rent = item.css("td::text").get().strip().split(" ")[0]
                    if "," in rent:
                        rent_array = rent.split(",")
                        rent = rent_array[0] + rent_array[1]
                elif "Balcone:" in item.css("strong::text").get():
                    balcony = item.css("td::text").get().strip()
                    if "No" in balcony:
                        balcony = False
                    else:
                        balcony = True
                elif "Terrazza:" in item.css("strong::text").get():
                    terrace_temp = item.css("td::text").get().strip()
                    if "No" in terrace_temp:
                        terrace = False
                    else:
                        terrace = True
                elif "Posto Auto:" in item.css("strong::text").get():
                    parking_temp = item.css("td::text").get().strip()
                    if "No" in parking_temp:
                        parking = False
                    else:
                        parking = True
                elif "Ascensore:" in item.css("strong::text").get():
                    elevator = item.css("td::text").get().strip()
                    if "No" in elevator:
                        elevator = False
                    else:
                        elevator = True
                elif "Vani:" in item.css("strong::text").get():
                    rooms = item.css("td::text").get().strip()
                elif "Spese Condominiali:" in item.css("strong::text").get():
                    utils = item.css("td::text").get().strip().split(" ")[0]
            except:
                pass

        try:
            date = date.split('-')
            date = date[2] + "-"+date[1]+"-"+date[0]
        except:
            pass

        # MetaData
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)

        # Property Details
        item_loader.add_value("property_type", "apartment")
        item_loader.add_value("square_meters", int(space))
        item_loader.add_value("room_count", rooms)
        item_loader.add_value("bathroom_count", bathrooms)
        item_loader.add_value("address", address)
        item_loader.add_value("city", city)
        item_loader.add_value("floor", floor)

        item_loader.add_value("furnished", furnished)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("terrace", terrace)
        item_loader.add_value("parking", parking)
        item_loader.add_value("elevator", elevator)
        item_loader.add_value("available_date", date)

        # Images
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))

        # Monetary Status
        item_loader.add_value("rent", int(rent))
        item_loader.add_value("utilities", utils)
        item_loader.add_value("currency", "EUR")

        # LandLord Details
        item_loader.add_value("landlord_phone", "39055332878")
        item_loader.add_value(
            "landlord_email", "granducatoimmobiliare@gmail.com")
        item_loader.add_value(
            "landlord_name", "GRANDUCATO IMMOBILIARE FIRENZE")

        yield item_loader.load_item()
