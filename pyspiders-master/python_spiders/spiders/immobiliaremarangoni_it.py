import scrapy
import re
from scrapy.http.request import Request
from ..loaders import ListingLoader


class ImmobiliaremarangoniItSpider(scrapy.Spider):
    name = "immobiliaremarangoni_it"
    allowed_domains = ["immobiliaremarangoni.it"]
    start_urls = [
        'https://www.immobiliaremarangoni.it/?sfid=13206&_sft_category=immobili-in-affitto&sf_paged=1',
    ]
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'development'

    # 1. FOLLOWING
    def parse(self, response, **kwargs):
        for appartment in response.xpath('//main/section/div/div/div/article/div/a/@href'):
            yield Request(appartment.get(), callback=self.populate_item, dont_filter=True)

        try:
            next_page = response.css(
                'body>div.l-canvas.sidebar_right.type_wide>div>div>main>section>div>div>nav>div>a.next.page-numbers').attrib['href']
        except:
            next_page = None

        if next_page is not None:
            yield response.follow(next_page, callback=self.parse)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        external_id = response.xpath(
            "//link[@rel='shortlink']/@href").get().split('=')[1]

        title = response.css('h1::text').get()

        rent = response.css("body>div.l-canvas.sidebar_right.type_wide>section:nth-child(2)>div.l-section-h.i-cf>div>div>div>div>div>div.vc_col-sm-9.wpb_column.vc_column_container>div>div>div.vc_acf.vc_txt_align_left.field_5c20b6cd4f232::text")
        rent = rent.get().split(" ")[1]
        try:
            rent = int(rent)
        except:
            return

        space = response.css("body>div.l-canvas.sidebar_right.type_wide>section:nth-child(2)>div.l-section-h.i-cf>div>div>div>div>div>div.vc_col-sm-9.wpb_column.vc_column_container>div>div>div.vc_acf.vc_txt_align_left.field_5c20b50a7305c::text").get()
        rooms = response.css(
            "div.icon_description > h3.ult-responsive.info-list-heading::text")[4].get()[0]

        try:
            rooms = int(rooms)
        except:
            return

        address = response.css('body > div.l-canvas.sidebar_right.type_wide > section:nth-child(2) > div.l-section-h.i-cf > div > div > div > div > div > div.vc_col-sm-9.wpb_column.vc_column_container > div > div > div.vc_acf.vc_txt_align_left.field_5c66b1279c99c::text').get()
        description_text = ''
        description = response.css(
            'div.wpb_wrapper > p ::text').extract()[:-8]

        for text in description:
            description_text += text

        description = ''.join(description_text)

        bathrooms = response.css(
            "div.icon_description_text.ult-responsive > p::text")[5].get()[-1]

        floor_Data = response.css(
            "div.icon_description_text.ult-responsive > p::text")[7].get()

        elevator_data = response.css(
            "div.icon_description_text.ult-responsive > p::text")[9].get()

        floor = None

        try:
            if "terra" in floor_Data.lower():
                floor = 1
            else:
                floor = int(floor_Data[0])
        except:
            floor = None

        elevator = None

        if "ascensore: no" not in elevator_data.lower():
            elevator = True
        if "ascensore: no" in elevator_data.lower():
            elevator = False

        images = response.css(
            'img.rsTmb::attr(src)').extract()

        for i in range(len(images)):
            images[i] = re.sub(r'-\d*x\d*', "", images[i])

        features = response.css('li.icon_list_item')

        energy = None
        furnished = None
        utils = None
        balcony = None
        availableDate = None
        for item in features:
            if "Certificazione Energetica" in item.css("div.icon_description > h3::text").get():
                energy = item.css(
                    "div.icon_description_text.ult-responsive > p::text").get()
            elif "Arredamento" in item.css("div.icon_description > h3::text").get():
                furnished = item.css(
                    "div.icon_description_text.ult-responsive > p::text").get()
                if "arredato" in furnished.lower() or 'completo' in furnished.lower():
                    furnished = True
            elif "Spese Condominiali" in item.css("div.icon_description > h3::text").get():
                utils = item.css(
                    "div.icon_description_text.ult-responsive > p::text").get().split(' ')[0]
            elif "5 Locali" in item.css("div.icon_description > h3::text").get():
                items = item.css(
                    "div.icon_description_text.ult-responsive > p::text").extract()
                for item in items:
                    if "Balconi" in item and "si" in item:
                        balcony = True
            elif "Libero da" in item.css("div.icon_description > h3::text").get():
                availableDate = item.css(
                    "div.icon_description_text.ult-responsive > p::text").get().split(' ')[0]

        if "in fase di rilascio" in energy or "In attesa di rilascio" in energy:
            energy = None
        else:
            energy = energy[-1]

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
        item_loader.add_value("city", "Torino")
        item_loader.add_value("energy_label", energy)
        item_loader.add_value("furnished", furnished)
        item_loader.add_value("balcony", balcony)

        if elevator != None:
            item_loader.add_value("elevator", elevator)
        if floor != None:
            item_loader.add_value("floor", str(floor))

        item_loader.add_value("available_date", availableDate)

        # Images
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))

        # Monetary Status
        item_loader.add_value("rent", rent)
        item_loader.add_value("utilities", utils)
        item_loader.add_value("currency", "EUR")

        # LandLord Details
        item_loader.add_value("landlord_phone", "00390113828650")
        item_loader.add_value("landlord_email", "info@immobiliaremarangoni.it")
        item_loader.add_value("landlord_name", "Marangoni Immobilire")

        yield item_loader.load_item()
